"""Controle qualite independant de trips_history.jsonl et stations.json, en Python pur (sans Spark).

Sert de REFERENCE pour valider les resultats RDD / DataFrames (Parties 2 et 3) :
chiffre d'affaires par ville, duree moyenne par ville et type de vehicule, top 5 des stations de depart.

Usage : python data/validate_data.py
"""
import json
from collections import Counter, defaultdict
from pathlib import Path

D = Path(__file__).parent


def reference(trips_path=D / "trips_history.jsonl", stations_path=D / "stations.json") -> dict:
    stations = {s["station_id"]: s for s in json.loads(Path(stations_path).read_text(encoding="utf8"))}
    events = [json.loads(l) for l in Path(trips_path).read_text(encoding="utf8").splitlines() if l.strip()]

    seen, clean = set(), []
    for e in events:                                   # dedoublonnage sur (trip_id, status)
        key = (e["trip_id"], e["status"])
        if key not in seen:
            seen.add(key)
            clean.append(e)
    ended = [e for e in clean if e["status"] == "ENDED"]

    revenue = defaultdict(float)
    dur = defaultdict(lambda: [0, 0])
    for e in ended:
        revenue[e["city"]] += e["price_mad"]
        s = dur[(e["city"], e["vehicle_type"])]
        s[0] += e["duration_s"]
        s[1] += 1
    return {
        "evenements": len(events), "doublons": len(events) - len(clean), "stations": len(stations),
        "statuts": dict(Counter(e["status"] for e in clean)),
        "trajets_termines": len(ended),
        "chiffre_affaires_par_ville": {k: round(v, 2) for k, v in sorted(revenue.items())},
        "duree_moyenne_s_par_ville_type": {f"{c}/{t}": round(v[0] / v[1], 1) for (c, t), v in sorted(dur.items())},
        "top5_stations_depart": sorted(Counter(e["start_station"] for e in ended).items(), key=lambda kv: (-kv[1], kv[0]))[:5],
        "inconnues": sum(1 for e in events if e["start_station"] not in stations),
    }


if __name__ == "__main__":
    for k, v in reference().items():
        print(f"{k}: {v}")
