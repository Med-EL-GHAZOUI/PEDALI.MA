"""Teste la Partie 4 (watermark, retards, doublons, modes update/append) sans Kafka.

Des lots de trajets sont envoyes un par un sur un socket TCP (une ligne JSON par trajet) ; trois requetes (update, append, complete)
lisent le meme flux. Le resultat est compare a une simulation Python independante de la regle du watermark.

Usage : python tests/check_streaming.py   (depuis la racine du depot ; pyspark requis)
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "streaming"))
sys.path.insert(0, str(ROOT / "data"))

from pyspark.sql import SparkSession
from stream_kpi import kpi_stream, known_station_ids, read_socket

WM = timedelta(minutes=10)
WIN = timedelta(minutes=15)
T0 = datetime(2026, 10, 15)


def at(hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return T0 + timedelta(hours=h, minutes=m)


def trip(tid, hhmm, status="completed", price=22.5, dur=15, user="U1", station="S01"):
    start = at(hhmm)
    done = status == "completed"
    return {"trip_id": tid, "user_id": user, "bike_id": "B1", "start_station_id": station,
            "end_station_id": "S02" if done else None, "city": "Casablanca",
            "start_time": start.isoformat(),
            "end_time": (start + timedelta(minutes=dur)).isoformat() if done else None,
            "duration_min": dur if done else None, "price_mad": price if done else 0.0, "status": status}


# Chaque lot = un fichier = un micro-batch.
BATCHES = [
    # B1 : max start_time = 10:20 -> watermark 10:10 pour le lot suivant
    [trip("T1", "10:01"), trip("T2", "10:05"), trip("T3", "10:12", "cancelled"), trip("T4", "10:20")],
    # B2 (watermark courant 10:10) : T5 (10:11 >= 10:10) arrive en retard de 9 min mais ACCEPTE ;
    #      T5x (10:08 < 10:10) est ECARTE alors que sa fenetre 10:00-10:15 est encore ouverte ; T1 rejoue -> DOUBLON ;
    #      max = 10:41 -> watermark 10:31
    [trip("T5", "10:11"), trip("T5x", "10:08"), trip("T1", "10:01"), trip("T6", "10:41")],
    # B3 (watermark courant 10:31) : T7 (10:03) et T8 (10:29) < 10:31 -> TROP EN RETARD ; 3 lignes invalides ; T9 10:50
    [trip("T7", "10:03"), trip("T8", "10:29"), trip("T9", "10:50", "breakdown"),
     trip("T10", "10:55", user=None), trip("T11", "10:56", dur=-5), trip("T12", "10:57", station="S99")],
    # B4 : fait avancer le watermark (11:20 - 10 min = 11:10) pour fermer les fenetres jusqu'a 11:00
    [trip("T13", "11:20")],
]
BAD_ROWS = {"T10", "T11", "T12"}  # rejetes par les regles de qualite


def win_start(t: datetime) -> datetime:
    return T0 + ((t - T0) // WIN) * WIN


def simulate():
    """Reference : applique les regles a la main, lot par lot. Retourne (etat final par fenetre, ids retenus, ids ecartes)."""
    wm, seen, kept, dropped = None, set(), [], []
    max_t = None
    for batch in BATCHES:
        for r in batch:
            t = datetime.fromisoformat(r["start_time"])
            if r["trip_id"] in BAD_ROWS:
                dropped.append((r["trip_id"], "qualite"))
            elif r["trip_id"] in seen:
                dropped.append((r["trip_id"], "doublon"))
            elif wm is not None and t < wm:  # regle observee : start_time < watermark -> ecarte (le dedoublonnage filtre deja)
                dropped.append((r["trip_id"], "trop en retard"))
            else:
                seen.add(r["trip_id"])
                kept.append(r)
        bt = max(datetime.fromisoformat(r["start_time"]) for r in batch)
        max_t = bt if max_t is None else max(max_t, bt)
        wm = max_t - WM  # applique a partir du lot suivant
    agg = defaultdict(lambda: {"valides": 0, "termines": 0, "ca": 0.0})
    for r in kept:
        a = agg[win_start(datetime.fromisoformat(r["start_time"]))]
        a["valides"] += 1
        if r["status"] == "completed":
            a["termines"] += 1
            a["ca"] += r["price_mad"]
    return dict(agg), {r["trip_id"] for r in kept}, dropped, wm


def as_state(rows):
    return {r.debut: {"valides": r.trajets_valides, "termines": r.trajets_termines, "ca": r.chiffre_affaires or 0.0} for r in rows}


def main():
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]")).appName("check-streaming")
             .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    tmp = Path(tempfile.mkdtemp(prefix="pedali-stream-"))
    modes = ["update", "append", "complete"]

    # Serveur TCP : chaque requete Spark s'y connecte ; chaque lot est envoye a toutes les connexions.
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(len(modes))
    port = server.getsockname()[1]
    conns = []
    ready = threading.Event()

    def accept_all():
        for _ in modes:
            conns.append(server.accept()[0])
        ready.set()

    threading.Thread(target=accept_all, daemon=True).start()

    emitted = {m: [] for m in modes}  # [(batch_id, [lignes])]

    def sink(mode):
        return lambda df, bid: emitted[mode].append((bid, df.collect()))

    known = known_station_ids(spark)
    queries = {}
    for m in modes:
        out = kpi_stream(read_socket(spark, "127.0.0.1", port), known)
        queries[m] = (out.writeStream.outputMode(m).foreachBatch(sink(m))
                      .option("checkpointLocation", str(tmp / f"ckpt_{m}")).start())
    assert ready.wait(60), "Spark ne s'est pas connecte au socket"

    for batch in BATCHES:
        payload = ("\n".join(json.dumps(r) for r in batch) + "\n").encode("utf8")
        for c in conns:
            c.sendall(payload)
        time.sleep(1.5)  # laisse la source socket lire le lot avant de lancer le micro-batch
        for q in queries.values():
            q.processAllAvailable()
    for q in queries.values():
        q.processAllAvailable()  # micro-batch sans donnees : emet les fenetres fermees par le dernier watermark
    for q in queries.values():
        q.stop()
    for c in conns:
        c.close()
    server.close()

    expected, kept_ids, dropped, final_wm = simulate()
    print("Watermark final :", final_wm.strftime("%H:%M"))
    print("Ecartes :", dropped)
    for mode, batches in emitted.items():
        print(f"\n== mode {mode} ==")
        for bid, rows in batches:
            if rows:
                print(f"  batch {bid}: " + "; ".join(f"{r.debut:%H:%M}-{r.fin:%H:%M} valides={r.trajets_valides} termines={r.trajets_termines}"
                                                     for r in sorted(rows, key=lambda r: r.debut)))
            else:
                print(f"  batch {bid}: (rien)")

    # --- UPDATE : le dernier etat vu de chaque fenetre doit etre egal a l'attendu (T7/T8/T1-doublon/invalides exclus)
    last = {}
    for _, rows in emitted["update"]:
        last.update(as_state(rows))
    assert last == expected, f"update : {last} != {expected}"
    # fenetre 10:00 : T1, T2, T3, T5 = 4 valides, 3 termines (T5x, T7 en retard et doublon de T1 absents)
    assert expected[at("10:00")] == {"valides": 4, "termines": 3, "ca": 67.5}, expected[at("10:00")]

    # --- APPEND : chaque fenetre fermee par le watermark est emise une seule fois, avec son etat final
    app = [as_state(rows) for _, rows in emitted["append"] if rows]
    flat = {}
    for s in app:
        for k, v in s.items():
            assert k not in flat, f"fenetre {k} emise deux fois en append"
            flat[k] = v
    closed = {w: v for w, v in expected.items() if w + WIN <= final_wm}
    assert flat == closed, f"append : {flat} != {closed}"
    assert at("11:15") not in flat, "fenetre encore ouverte emise trop tot en append"
    print("\nOK : retards toleres/rejetes, doublons, donnees invalides et modes update/append/complete conformes a la simulation")
    spark.stop()


if __name__ == "__main__":
    main()
