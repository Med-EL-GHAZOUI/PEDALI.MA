"""Partie 4 : Structured Streaming depuis Kafka (topic trips).

  1. lecture du topic avec spark.readStream.format("kafka")
  2. conversion de la colonne value en colonnes typees avec from_json et le schema de la Partie 3
  3. flux brut affiche dans un sink console (outputMode "append")
  4. en parallele, flux brut (JSON) ecrit dans s3a://lake/bronze/trips_raw/ avec son checkpoint
  5. nombre de trajets par ville et par statut en console (outputMode "complete")
  6. watermark de 10 minutes sur event_time (fenetres de 10 minutes par ville, mode "update")

Usage (Jupyter / cluster) :
    python pipeline/part4_streaming.py --seconds 90
Les sorties des sinks console apparaissent sur la sortie standard du driver.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, "/home/jovyan/work/data")
sys.path.insert(0, "/home/jovyan/work/pipeline")
from pedali_schema import TRIPS_SCHEMA
from pedali_spark import get_spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

KAFKA = os.environ.get("KAFKA_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "trips")
BRONZE_RAW = "s3a://lake/bronze/trips_raw"
CKPT = "s3a://lake/checkpoints"
TRIGGER = "5 seconds"


def read_kafka(spark: SparkSession, topic: str = TOPIC, starting_offsets: str = "earliest", max_offsets: int = 1000) -> DataFrame:
    """Flux brut Kafka : key, value (binaires), topic, partition, offset, timestamp."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA).option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("maxOffsetsPerTrigger", max_offsets)          # taille maximale d'un micro-batch
            .option("failOnDataLoss", "false").load())


def parse_events(raw: DataFrame) -> DataFrame:
    """value (JSON) -> colonnes typees ; un message illisible donne une ligne dont tous les champs JSON sont null."""
    return (raw.select(F.col("value").cast("string").alias("json"), "partition", "offset", F.col("timestamp").alias("kafka_ts"))
            .withColumn("t", F.from_json("json", TRIPS_SCHEMA))
            .select("t.*", "partition", "offset", "kafka_ts"))


def start_console_raw(parsed: DataFrame):
    """3. Flux brut dans la console (append : chaque micro-batch n'affiche que ses nouvelles lignes)."""
    cols = ["trip_id", "vehicle_type", "city", "status", "duration_s", "price_mad", "event_time", "partition", "offset"]
    return (parsed.select(*cols).writeStream.format("console").outputMode("append")
            .option("truncate", False).option("numRows", 8)
            .trigger(processingTime=TRIGGER).queryName("console_raw").start())


def start_bronze_raw(raw: DataFrame, path: str = BRONZE_RAW):
    """4. JSON bruts tels que lus dans Kafka (cle, valeur, partition, offset) dans la couche Bronze."""
    out = raw.select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                     "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"))
    return (out.writeStream.format("json").outputMode("append").option("path", path)
            .option("checkpointLocation", f"{CKPT}/p4_bronze_raw")
            .trigger(processingTime=TRIGGER).queryName("bronze_raw").start())


def start_counts(parsed: DataFrame):
    """5. Nombre de trajets (evenements) par ville et par statut : agregation d'etat, mode complete."""
    counts = parsed.filter("trip_id IS NOT NULL").groupBy("city", "status").count()
    return (counts.writeStream.format("console").outputMode("complete").option("truncate", False).option("numRows", 20)
            .trigger(processingTime=TRIGGER).queryName("counts_city_status").start())


def start_watermark_windows(parsed: DataFrame, watermark: str = "10 minutes", window: str = "10 minutes"):
    """6. Fenetres de 10 min par ville avec watermark de 10 min sur event_time (mode update)."""
    agg = (parsed.filter("trip_id IS NOT NULL").withWatermark("event_time", watermark)
           .groupBy(F.window("event_time", window).alias("fenetre"), "city").count())
    return (agg.writeStream.format("console").outputMode("update").option("truncate", False).option("numRows", 8)
            .trigger(processingTime=TRIGGER).queryName("wm_windows").start())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=90)
    ap.add_argument("--max-offsets", type=int, default=1000)
    ap.add_argument("--starting-offsets", default="earliest")
    args = ap.parse_args()

    spark = get_spark("pedali-part4-streaming")
    spark.sparkContext.setLogLevel("ERROR")
    raw = read_kafka(spark, starting_offsets=args.starting_offsets, max_offsets=args.max_offsets)
    parsed = parse_events(raw)
    queries = [start_console_raw(parsed), start_bronze_raw(raw), start_counts(parsed), start_watermark_windows(parsed)]
    end = time.time() + args.seconds
    while time.time() < end:
        time.sleep(10)
        for q in queries:
            p = q.lastProgress
            if p:
                print(f"### {q.name:20s} micro-batch {p['batchId']:>3} : {p['numInputRows']:>5} lignes, "
                      f"{p['processedRowsPerSecond']:.0f} lignes/s, trigger {p['durationMs'].get('triggerExecution')} ms", flush=True)
    for q in queries:
        q.stop()
    spark.stop()


if __name__ == "__main__":
    main()
