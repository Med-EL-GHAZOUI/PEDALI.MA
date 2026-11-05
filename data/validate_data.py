"""Controle qualite independant (Personne 2) de trips_history.jsonl et stations.json.

Sert aussi de reference pour valider les resultats RDD / DataFrames (Parties 2 et 3) :
il calcule en Python pur revenus par ville, duree moyenne et top 5 des stations.

Usage : python data/validate_data.py
"""
import json
from collections import Counter, defaultdict
from pathlib import Path

D = Path(__file__).parent
stations = {s["station_id"]: s for s in json.loads((D / "stations.json").read_text(encoding="utf8"))}
trips = [json.loads(l) for l in (D / "trips_history.jsonl").read_text(encoding="utf8").splitlines() if l.strip()]

ids = Counter(t["trip_id"] for t in trips)
dup_ids = {k for k, v in ids.items() if v > 1}
rules = {
    "doublons (trip_id repete)": sum(1 for t in trips if t["trip_id"] in dup_ids),
    "user_id manquant": sum(1 for t in trips if not t["user_id"]),
    "station de depart inconnue": sum(1 for t in trips if t["start_station_id"] not in stations),
    "duree negative": sum(1 for t in trips if t["duration_min"] is not None and t["duration_min"] < 0),
    "trajet termine sans end_time": sum(1 for t in trips if t["status"] == "completed" and not t["end_time"]),
}
print(f"{len(trips)} lignes, {len(stations)} stations, {len(dup_ids)} trip_id dupliques")
for k, v in rules.items():
    print(f"  [{'OK' if v == 0 else '!!'}] {k}: {v}")

seen, clean = set(), []
for t in trips:
    if t["trip_id"] in seen or not t["user_id"] or t["start_station_id"] not in stations:
        continue
    if t["duration_min"] is not None and t["duration_min"] < 0:
        continue
    seen.add(t["trip_id"])
    if t["status"] == "completed":
        clean.append(t)

rev = defaultdict(float)
for t in clean:
    rev[t["city"]] += t["price_mad"]
print(f"\nReference apres nettoyage ({len(clean)} trajets termines)")
print("Revenus par ville :", {k: round(v, 2) for k, v in sorted(rev.items())})
print("Duree moyenne (min):", round(sum(t["duration_min"] for t in clean) / len(clean), 2))
print("Top 5 stations de depart:", Counter(t["start_station_id"] for t in clean).most_common(5))
