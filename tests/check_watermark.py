"""Partie 4 : que devient un evenement qui arrive avec 15 minutes de retard ? (watermark de 10 minutes)

Scenario sur un topic Kafka dedie (jetable), fenetres de 10 minutes sur event_time :
  lot 1 : evenements a 11:42 et 11:44                 -> fenetre [11:40, 11:50) : 2 evenements
  lot 2 : un evenement a 12:00                         -> le watermark passe a 12:00 - 10 min = 11:50
  lot 3 : un evenement a 11:45 (15 min de retard)      -> fenetre [11:40, 11:50) deja fermee (fin <= watermark) : IGNORE
          un evenement a 11:55 (5 min de retard)       -> fenetre [11:50, 12:00) encore ouverte : ACCEPTE
En mode append, la fenetre [11:40, 11:50) est emise une seule fois, avec 2 evenements : la preuve que l'evenement
en retard de 15 minutes a ete ecarte.

Usage (Jupyter) : python tests/check_watermark.py
"""
import json
import sys
import time

sys.path.insert(0, "/home/jovyan/work/pipeline")
sys.path.insert(0, "/home/jovyan/work/data")
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pedali_spark import get_spark
import part4_streaming as p4
from pyspark.sql import functions as F

BOOTSTRAP = "kafka:9092"
TOPIC = f"trips_wm_demo_{int(time.time())}"


def event(trip_id: str, hhmm: str) -> dict:
    return {"trip_id": trip_id, "user_id": "U-00001", "vehicle_id": "V-BK-0001", "vehicle_type": "BIKE",
            "start_station": "S-CAS-001", "end_station": None, "city": "Casablanca", "status": "RESERVED",
            "duration_s": None, "distance_km": None, "battery_pct": None, "price_mad": None,
            "event_time": f"2026-11-15T{hhmm}:00Z"}


def main():
    KafkaAdminClient(bootstrap_servers=BOOTSTRAP).create_topics([NewTopic(TOPIC, 1, 1)])
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode())

    spark = get_spark("pedali-watermark-demo")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    parsed = p4.parse_events(p4.read_kafka(spark, topic=TOPIC, max_offsets=1000))
    agg = (parsed.withWatermark("event_time", "10 minutes")
           .groupBy(F.window("event_time", "10 minutes").alias("fenetre")).count())
    q_update = (agg.writeStream.format("console").outputMode("update").option("truncate", False)
                .trigger(processingTime="1 second").queryName("wm_update").start())
    q_append = (agg.writeStream.format("memory").queryName("wm_append").outputMode("append")
                .trigger(processingTime="1 second").start())

    def send(label, *events):
        for e in events:
            producer.send(TOPIC, e)
        producer.flush()
        print(f"\n######## {label} : envoi de {[e['event_time'][11:16] for e in events]}", flush=True)
        q_update.processAllAvailable()
        q_append.processAllAvailable()
        ev = (q_update.lastProgress or {}).get("eventTime", {})
        print(f"######## event time vu par Spark : max={ev.get('max')} watermark={ev.get('watermark')}", flush=True)

    send("lot 1", event("T-W1", "11:42"), event("T-W2", "11:44"))
    send("lot 2", event("T-W3", "12:00"))
    send("lot 3 (11:45 = 15 min de retard, 11:55 = 5 min de retard)", event("T-LATE15", "11:45"), event("T-LATE5", "11:55"))
    send("lot 4 (fait avancer le watermark a 12:10)", event("T-W4", "12:20"))

    time.sleep(8)          # lot vide : Spark emet (mode append) les fenetres fermees par le nouveau watermark
    q_append.processAllAvailable()
    print("\n######## Table append (fenetres FERMEES, emises une seule fois) :")
    rows = spark.sql("SELECT fenetre.start AS debut, fenetre.end AS fin, count FROM wm_append ORDER BY debut").collect()
    for r in rows:
        print(f"  {r.debut} -> {r.fin} : {r['count']} evenement(s)")
    by_start = {str(r.debut)[11:16]: r["count"] for r in rows}
    assert by_start.get("11:40") == 2, "l'evenement en retard de 15 min aurait du etre ecarte"
    assert by_start.get("11:50") == 1, "l'evenement en retard de 5 min aurait du etre accepte"
    print("OK : retard de 15 min ecarte (fenetre 11:40 = 2), retard de 5 min accepte (fenetre 11:50 = 1)")
    q_update.stop()
    q_append.stop()
    spark.stop()


if __name__ == "__main__":
    main()
