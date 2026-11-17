"""Generateur d'evenements Pedali.ma (format du sujet) : fichier historique ou flux Kafka.

NOTE : le sujet mentionne un producer.py "fourni". Ce fichier le reproduit a partir de la description
(statuts, champs, ~5 % d'evenements en retard, ~2 % de doublons, scenarios injectes). A remplacer par le
fichier officiel des qu'il est disponible : les noms de champs sont ceux du sujet.

Un evenement JSON par changement d'etat : RESERVED -> UNLOCKED -> ENDED | FAULT_REPORTED, ou RESERVED -> CANCELLED.
`end_station`, `duration_s`, `distance_km`, `price_mad` ne sont renseignes que pour ENDED ; `battery_pct` est null pour un velo classique.

Scenarios injectes :
  - flux pendulaires        : residentiel -> centre d'affaires le matin, sens inverse le soir
  - station qui se vide     : S-CAS-005 (Casablanca) : beaucoup de departs, jamais d'arrivee
  - boucle touristique      : A -> B -> C -> A entre 3 stations "tourisme" de Marrakech
  - vehicule en panne       : V-SC-0666 (Tanger) accumule les signalements FAULT_REPORTED

Usage :
    python producer/producer.py --mode file --n 50000                 # ecrit data/trips_history.jsonl (50 000 evenements)
    python producer/producer.py --mode kafka --rate 20                # 20 evenements/s vers le topic trips
    python producer/producer.py --mode kafka --bootstrap kafka:9092   # depuis un conteneur
"""
import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
EMPTYING_STATION = "S-CAS-005"
TOURIST_CITY = "Marrakech"
FAULTY_VEHICLE = ("V-SC-0666", "SCOOTER", "Tanger")
CITY_WEIGHTS = {"Casablanca": 35, "Rabat": 20, "Marrakech": 25, "Tanger": 20}
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 3, 8, 12, 10, 6, 5, 6, 7, 6, 5, 6, 8, 12, 10, 6, 4, 3, 2, 1]
SPEED_KMH = {"BIKE": 12, "EBIKE": 18, "SCOOTER": 20}
PRICE_PER_MIN = {"BIKE": 0.5, "EBIKE": 0.7, "SCOOTER": 0.8}
PREFIX = {"BIKE": "BK", "EBIKE": "EB", "SCOOTER": "SC"}
FMT = "%Y-%m-%dT%H:%M:%SZ"


def load_stations(path: Path):
    stations = json.loads(path.read_text(encoding="utf8"))
    by_city, by_zone = {}, {}
    for s in stations:
        by_city.setdefault(s["city"], []).append(s["station_id"])
        by_zone.setdefault((s["city"], s["zone_type"]), []).append(s["station_id"])
    city_of = {s["station_id"]: s["city"] for s in stations}
    return by_city, by_zone, city_of


class Generator:
    def __init__(self, stations_path: Path, seed: int, start: datetime, days: int):
        self.rng = random.Random(seed)
        self.by_city, self.by_zone, self.city_of = load_stations(stations_path)
        self.start, self.days = start, days
        self.seq = 0

    def _route(self, city: str, hour: int):
        r, rng = self.rng.random(), self.rng
        zone = lambda z: self.by_zone.get((city, z)) or self.by_city[city]
        if 6 <= hour <= 9 and r < 0.35:                                  # pendulaires du matin
            return rng.choice(zone("residentiel")), rng.choice(zone("centre_affaires"))
        if 17 <= hour <= 20 and r < 0.35:                                # pendulaires du soir
            return rng.choice(zone("centre_affaires")), rng.choice(zone("residentiel"))
        if city == TOURIST_CITY and r < 0.10:                            # boucle A -> B -> C -> A
            loop = self.by_zone[(city, "tourisme")][:3]
            i = rng.randint(0, 2)
            return loop[i], loop[(i + 1) % 3]
        if city == "Casablanca" and r > 0.93:                            # station qui se vide
            return EMPTYING_STATION, rng.choice([s for s in self.by_city[city] if s != EMPTYING_STATION])
        pool = [s for s in self.by_city[city] if s != EMPTYING_STATION]
        a, b = rng.sample(pool, 2)
        return a, b

    def _event(self, trip, status, when, **extra):
        e = {"trip_id": trip["trip_id"], "user_id": trip["user_id"], "vehicle_id": trip["vehicle_id"],
             "vehicle_type": trip["vehicle_type"], "start_station": trip["start_station"], "end_station": None,
             "city": trip["city"], "status": status, "duration_s": None, "distance_km": None,
             "battery_pct": trip["battery_pct"], "price_mad": None, "event_time": when.strftime(FMT)}
        e.update(extra)
        return e

    def trip(self, force_fault: bool = False):
        rng = self.rng
        self.seq += 1
        if force_fault:
            vid, vtype, city = FAULTY_VEHICLE
        else:
            city = rng.choices(list(CITY_WEIGHTS), weights=list(CITY_WEIGHTS.values()))[0]
            vtype = rng.choices(["BIKE", "EBIKE", "SCOOTER"], weights=[50, 30, 20])[0]
            vid = f"V-{PREFIX[vtype]}-{rng.randint(1, 999):04d}"
        hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        t0 = self.start + timedelta(days=rng.randrange(self.days), hours=hour, minutes=rng.randrange(60), seconds=rng.randrange(60))
        a, b = self._route(city, hour)
        trip = {"trip_id": f"T-{self.seq:08d}", "user_id": f"U-{rng.randint(1, 30000):05d}", "vehicle_id": vid,
                "vehicle_type": vtype, "start_station": a, "city": city,
                "battery_pct": None if vtype == "BIKE" else rng.randint(20, 100)}
        events = [self._event(trip, "RESERVED", t0)]
        t_unlock = t0 + timedelta(seconds=rng.randint(60, 300))
        if not force_fault and rng.random() < 0.07:
            return [*events, self._event(trip, "CANCELLED", t0 + timedelta(seconds=rng.randint(30, 240)))]
        events.append(self._event(trip, "UNLOCKED", t_unlock))
        duration = max(120, int(rng.gauss(1100, 450)))
        if force_fault or rng.random() < 0.04:
            events.append(self._event(trip, "FAULT_REPORTED", t_unlock + timedelta(seconds=rng.randint(30, max(60, duration)))))
            return events
        km = round(duration / 3600 * SPEED_KMH[vtype] * rng.uniform(0.8, 1.2), 2)
        price = round(3 + duration / 60 * PRICE_PER_MIN[vtype], 2)
        events.append(self._event(trip, "ENDED", t_unlock + timedelta(seconds=duration), end_station=b,
                                  duration_s=duration, distance_km=km, price_mad=price))
        return events

    def history(self, n_events: int):
        """~n_events evenements, ranges par heure d'ARRIVEE : 5 % arrivent en retard (jusqu'a 15 min), 2 % en double."""
        rng = self.rng
        trips = [self.trip() for _ in range(int(n_events / 2.9))]
        trips += [self.trip(force_fault=True) for _ in range(35)]
        arrivals = []
        for events in trips:
            for e in events:
                when = datetime.strptime(e["event_time"], FMT)
                delay = rng.uniform(60, 900) if rng.random() < 0.05 else rng.uniform(0, 5)
                arrivals.append((when + timedelta(seconds=delay), e))
                if rng.random() < 0.02:                                    # doublon : meme trip_id, meme statut
                    arrivals.append((when + timedelta(seconds=delay + rng.uniform(1, 30)), dict(e)))
        arrivals.sort(key=lambda x: x[0])
        return [e for _, e in arrivals[:n_events]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000, help="nombre d'evenements")
    ap.add_argument("--rate", type=float, default=20, help="evenements/seconde (mode kafka)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--out", default=str(DATA / "trips_history.jsonl"))
    ap.add_argument("--stations", default=str(DATA / "stations.json"))
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="trips")
    args = ap.parse_args()

    if not Path(args.stations).exists():
        raise SystemExit(f"{args.stations} introuvable : lancer d'abord python data/generate_stations.py")
    start = datetime(2026, 10, 17, tzinfo=timezone.utc)
    gen = Generator(Path(args.stations), args.seed, start, args.days)
    events = gen.history(args.n)

    if args.mode == "file":
        Path(args.out).write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events), encoding="utf8")
        print(f"{len(events)} evenements ecrits dans {args.out}")
        return

    from kafka import KafkaProducer                       # import tardif : inutile en mode fichier
    # on decale les dates pour que le dernier evenement soit "maintenant" (watermark / fenetres coherents)
    shift = datetime.now(timezone.utc) - datetime.strptime(events[-1]["event_time"], FMT).replace(tzinfo=timezone.utc)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", linger_ms=20,
                             key_serializer=lambda k: k.encode(), value_serializer=lambda v: v.encode())
    for i, e in enumerate(events, 1):
        e = dict(e, event_time=(datetime.strptime(e["event_time"], FMT) + shift).strftime(FMT))
        producer.send(args.topic, key=e["trip_id"], value=json.dumps(e, ensure_ascii=False))
        if i % 500 == 0:
            print(f"{i} evenements envoyes", flush=True)
        time.sleep(1 / args.rate)
    producer.flush()
    print(f"{len(events)} evenements envoyes vers {args.bootstrap}/{args.topic}")


if __name__ == "__main__":
    main()
