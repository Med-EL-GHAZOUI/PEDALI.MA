"""Partie 5 (Personne 2) - couche Gold en Delta Lake + test d'arret / reprise sur checkpoint.

Deroulement :
  1. requete streaming KPI -> table Delta (mode append, checkpoint) ; lots 1 et 2 traites ;
  2. ARRET de la requete et de la SparkSession ;
  3. depot des lots 3 et 4 pendant l'arret (dont un doublon et des retards) ;
  4. REPRISE : nouvelle session, meme checkpoint ; seuls les lots 3 et 4 doivent etre traites ;
  5. verification : la table Gold == simulation Python (aucune fenetre perdue ni dupliquee, etat de
     dedoublonnage / watermark restaure) ; affichage de DESCRIBE HISTORY.

Necessite Linux (checkpoint Hadoop) et le package Delta :
  python tests/check_delta_recovery.py [--gold /tmp/pedali-gold] [--history reports/delta_describe_history.txt]
Voir docs/partie5_delta.md pour la commande Docker.
"""
import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "streaming"))
sys.path.insert(0, str(ROOT / "data"))
sys.path.insert(0, str(ROOT / "tests"))

from pyspark.sql import SparkSession

from check_streaming import BATCHES, WIN, simulate
from stream_kpi import kpi_stream, known_station_ids, read_files

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


def new_session() -> SparkSession:
    return (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]")).appName("check-delta-recovery")
            .config("spark.jars.packages", DELTA_PACKAGE)
            .config("spark.jars.ivy", os.environ.get("IVY_DIR", "/tmp/ivy"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false").getOrCreate())


def run_query(inbox: Path, ckpt: Path, gold: Path):
    spark = new_session()
    spark.sparkContext.setLogLevel("ERROR")
    out = kpi_stream(read_files(spark, str(inbox)), known_station_ids(spark))
    q = (out.writeStream.format("delta").outputMode("append")
         .option("checkpointLocation", str(ckpt)).start(str(gold)))
    return spark, q


def drop_batch(inbox: Path, i: int):
    (inbox / f"batch_{i}.jsonl").write_text("\n".join(json.dumps(r) for r in BATCHES[i - 1]), encoding="utf8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", default="/tmp/pedali-gold")
    ap.add_argument("--history", default=str(ROOT / "reports/delta_describe_history.txt"))
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="pedali-delta-"))
    inbox, ckpt, gold = work / "in", work / "ckpt", Path(args.gold)
    inbox.mkdir()
    if gold.exists():
        shutil.rmtree(gold)

    # --- phase 1 : lots 1 et 2, puis arret
    spark, q = run_query(inbox, ckpt, gold)
    for i in (1, 2):
        drop_batch(inbox, i)
        q.processAllAvailable()
    processed_before = q.lastProgress["batchId"] if q.lastProgress else None
    q.stop()
    spark.stop()
    print(f"[phase 1] arret apres les lots 1-2 (dernier batchId = {processed_before})")

    # --- phase 2 : donnees deposees pendant l'arret
    for i in (3, 4):
        drop_batch(inbox, i)
    print("[phase 2] lots 3 et 4 deposes pendant l'arret")

    # --- phase 3 : reprise sur le meme checkpoint
    spark, q = run_query(inbox, ckpt, gold)
    q.processAllAvailable()
    q.processAllAvailable()  # lot sans donnees : emet les fenetres fermees par le dernier watermark
    resumed = [p for p in (q.recentProgress or []) if p.get("numInputRows", 0) > 0]
    q.stop()
    print(f"[phase 3] reprise : {len(resumed)} micro-batch(s) avec donnees, lignes lues = "
          f"{sum(p['numInputRows'] for p in resumed)} (attendu : seulement les lots 3-4)")

    gold_df = spark.read.format("delta").load(str(gold))
    rows = {r.debut: r for r in gold_df.collect()}
    n_rows = gold_df.count()

    expected, _, dropped, final_wm = simulate()
    closed = {w: v for w, v in expected.items() if w + WIN <= final_wm}
    got = {w: {"valides": r.trajets_valides, "termines": r.trajets_termines, "ca": r.chiffre_affaires or 0.0}
           for w, r in rows.items()}
    print("\nTable Gold apres reprise :")
    gold_df.orderBy("debut").show(truncate=False)

    history = spark.sql(f"DESCRIBE HISTORY delta.`{gold}`").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics")
    hist_text = history.orderBy("version")._jdf.showString(20, 120, False)
    print("DESCRIBE HISTORY :")
    print(hist_text)
    Path(args.history).parent.mkdir(parents=True, exist_ok=True)
    Path(args.history).write_text(hist_text, encoding="utf8")

    assert n_rows == len(closed), f"{n_rows} lignes Gold, attendu {len(closed)} (doublon ou fenetre manquante)"
    assert got == closed, f"Gold != simulation\n{got}\n{closed}"
    assert sum(p["numInputRows"] for p in resumed) <= sum(len(b) for b in BATCHES[2:]), \
        "la reprise a relu des lots deja traites"
    print("OK : arret/reprise sans perte ni doublon ; etat (dedoublonnage, watermark) restaure depuis le checkpoint")
    spark.stop()


if __name__ == "__main__":
    main()
