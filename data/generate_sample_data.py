"""Genere des donnees SYNTHETIQUES pour Pedali.ma (a remplacer par les vraies donnees du sujet).

Produit :
  data/stations.json         referentiel des stations
  data/trips_history.jsonl   un trajet JSON par ligne

Schema suppose (a confirmer avec le sujet, voir docs/schema_donnees.md).
Scenarios injectes (Partie 6) :
  - station qui se vide     : S01 (Casablanca) perd des velos en continu
  - boucle touristique      : S05 -> S06 -> S07 -> S05 (Marrakech)
  - flux pendulaires        : S10 (residentiel) -> S11 (affaires) le matin, inverse le soir
Anomalies de qualite injectees (a detecter en Partie 3/5) : doublons, valeurs nulles,
durees negatives, fin avant debut.

Usage : python data/generate_sample_data.py [nb_trajets] [graine]
"""
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).parent
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
random.seed(int(sys.argv[2]) if len(sys.argv) > 2 else 42)

STATIONS = [
    ("S01", "Casa Port", "Casablanca", 33.6020, -7.6180, 20),
    ("S02", "Maarif", "Casablanca", 33.5870, -7.6350, 25),
    ("S03", "Ain Diab", "Casablanca", 33.5950, -7.6700, 30),
    ("S04", "Hay Hassani", "Casablanca", 33.5650, -7.6600, 15),
    ("S05", "Jemaa el-Fna", "Marrakech", 31.6258, -7.9891, 30),
    ("S06", "Majorelle", "Marrakech", 31.6417, -8.0030, 20),
    ("S07", "Menara", "Marrakech", 31.6148, -8.0210, 20),
    ("S08", "Gueliz", "Marrakech", 31.6340, -8.0100, 25),
    ("S09", "Agdal Rabat", "Rabat", 33.9990, -6.8500, 20),
    ("S10", "Hay Riad", "Rabat", 33.9570, -6.8700, 20),
    ("S11", "Rabat Ville", "Rabat", 34.0200, -6.8300, 30),
    ("S12", "Kasbah Oudayas", "Rabat", 34.0300, -6.8350, 15),
]
CITY = {s[0]: s[2] for s in STATIONS}
BY_CITY = {}
for s in STATIONS:
    BY_CITY.setdefault(s[2], []).append(s[0])

PRICE_PER_MIN = 1.5  # MAD


def pick_route(hour):
    r = random.random()
    if r < 0.10:                                   # station qui se vide
        return "S01", random.choice(["S02", "S03", "S04"])
    if r < 0.18:                                   # boucle touristique
        loop = ["S05", "S06", "S07", "S05"]
        i = random.randint(0, 2)
        return loop[i], loop[i + 1]
    if r < 0.30:                                   # pendulaires
        return ("S10", "S11") if hour < 12 else ("S11", "S10")
    city = random.choice(list(BY_CITY))
    a, b = random.sample(BY_CITY[city], 2)
    return a, b


def make_trip(i, day0):
    hour = random.choices(range(24), weights=[1,1,1,1,1,3,8,12,10,6,5,6,7,6,5,6,8,12,10,6,4,3,2,1])[0]
    start = day0 + timedelta(days=random.randint(0, 29), hours=hour, minutes=random.randint(0, 59))
    a, b = pick_route(hour)
    duration = max(2, int(random.gauss(18, 8)))
    status = random.choices(["completed", "cancelled", "breakdown"], weights=[90, 6, 4])[0]
    end = start + timedelta(minutes=duration)
    return {
        "trip_id": f"T{i:07d}",
        "user_id": f"U{random.randint(1, 3000):05d}",
        "bike_id": f"B{random.randint(1, 400):04d}",
        "start_station_id": a,
        "end_station_id": b if status == "completed" else None,
        "city": CITY[a],
        "start_time": start.isoformat(timespec="seconds"),
        "end_time": end.isoformat(timespec="seconds") if status == "completed" else None,
        "duration_min": duration if status == "completed" else None,
        "price_mad": round(duration * PRICE_PER_MIN, 2) if status == "completed" else 0.0,
        "status": status,
    }


def inject_anomalies(trips):
    k = max(1, len(trips) // 200)
    for t in random.sample(trips, k):               # duree negative
        if t["duration_min"] is not None:
            t["duration_min"] = -abs(t["duration_min"])
    for t in random.sample(trips, k):               # station inconnue
        t["start_station_id"] = "S99"
    for t in random.sample(trips, k):               # user manquant
        t["user_id"] = None
    trips.extend(dict(t) for t in random.sample(trips, k))  # doublons


def main():
    day0 = datetime(2026, 10, 1)
    trips = [make_trip(i, day0) for i in range(1, N + 1)]
    inject_anomalies(trips)
    random.shuffle(trips)
    stations = [
        {"station_id": s[0], "name": s[1], "city": s[2], "lat": s[3], "lon": s[4], "capacity": s[5]}
        for s in STATIONS
    ]
    (OUT / "stations.json").write_text(json.dumps(stations, ensure_ascii=False, indent=2), encoding="utf8")
    with open(OUT / "trips_history.jsonl", "w", encoding="utf8") as f:
        for t in trips:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    print(f"{len(trips)} trajets, {len(stations)} stations ecrits dans {OUT}")


if __name__ == "__main__":
    main()
