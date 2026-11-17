"""Producteur Kafka (Personne 1) - rejoue trips_history.jsonl dans le topic `trips`.

Cle du message = start_station_id (les trajets d'une station restent dans une meme partition, donc ordonnes).
Les doublons du fichier sont volontairement conserves (reemission at-least-once, traitee en aval).

Usage :
    python producer/producer.py --limit 2000 --rate 500                  # depuis l'hote (localhost:29092)
    python producer/producer.py --bootstrap kafka:9092 --limit 2000      # depuis Jupyter / un conteneur
"""
import argparse
import json
import time
from pathlib import Path

from kafka import KafkaProducer

DEFAULT_FILE = Path(__file__).resolve().parent.parent / "data" / "trips_history.jsonl"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="trips")
    ap.add_argument("--file", default=str(DEFAULT_FILE))
    ap.add_argument("--rate", type=float, default=0, help="messages/seconde (0 = au plus vite)")
    ap.add_argument("--skip", type=int, default=0, help="ignorer les N premieres lignes")
    ap.add_argument("--limit", type=int, default=0, help="nombre max de messages (0 = tout le fichier)")
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", linger_ms=20,
                             key_serializer=lambda k: k.encode(), value_serializer=lambda v: v.encode())
    sent = 0
    with open(args.file, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i < args.skip:
                continue
            if args.limit and sent >= args.limit:
                break
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            producer.send(args.topic, key=record["start_station_id"], value=line)
            sent += 1
            if args.rate:
                time.sleep(1 / args.rate)
    producer.flush()
    print(f"{sent} messages envoyes vers {args.bootstrap}/{args.topic}")


if __name__ == "__main__":
    main()
