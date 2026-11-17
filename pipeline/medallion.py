"""Partie 5 (Personne 1) : pipeline Bronze -> Silver -> Gold en Delta Lake sur MinIO.

  Kafka `trips` --(append)--> BRONZE  s3a://lake/bronze/trips   brut + partition/offset/horodatages Kafka
  BRONZE (stream Delta) --foreachBatch MERGE--> SILVER s3a://lake/silver/trips
        parse JSON + regles de qualite (P2) + dedoublonnage sur trip_id (MERGE : rejouable sans doublon)
  SILVER (stream Delta) --foreachBatch--> GOLD s3a://lake/gold/kpi_city_day , gold/top_stations
        indicateurs recalcules sur toute la couche Silver (petit volume) avec les definitions de data/pedali_kpi.py

Chaque etape a son checkpoint (s3a://lake/checkpoints/<etape>) : apres un arret, la reprise continue
exactement la ou chaque etape s'est arretee (offsets Kafka pour Bronze, versions Delta pour Silver/Gold).
"""
import sys
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/home/jovyan/work/data")
from pedali_kpi import kpi_aggregations
from pedali_schema import TRIPS_SCHEMA

LAKE = "s3a://lake"
BRONZE = f"{LAKE}/bronze/trips"
SILVER = f"{LAKE}/silver/trips"
GOLD_KPI = f"{LAKE}/gold/kpi_city_day"
GOLD_TOP = f"{LAKE}/gold/top_stations"
CKPT = f"{LAKE}/checkpoints"
STATIONS = "file:///home/jovyan/work/data/stations.json"


def known_stations(spark: SparkSession) -> list:
    rows = spark.read.option("multiLine", True).json(STATIONS).select("station_id").collect()
    return [r[0] for r in rows]


# ---------------------------------------------------------------- BRONZE
def start_bronze(spark, servers="kafka:9092", topic="trips", trigger="3 seconds", max_offsets=5000):
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", servers)
           .option("subscribe", topic).option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", max_offsets).option("failOnDataLoss", "false").load()
           .select(F.col("key").cast("string").alias("cle"), F.col("value").cast("string").alias("value"),
                   "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"),
                   F.current_timestamp().alias("ingest_ts")))
    return (raw.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{CKPT}/bronze")
            .trigger(processingTime=trigger).queryName("bronze").start(BRONZE))


# ---------------------------------------------------------------- SILVER
def to_silver_rows(batch: DataFrame, stations: list) -> DataFrame:
    """JSON -> colonnes typees, regles de qualite, un seul exemplaire par trip_id."""
    parsed = batch.select(F.from_json("value", TRIPS_SCHEMA).alias("t")).select("t.*")
    return (parsed.filter(F.col("trip_id").isNotNull() & F.col("user_id").isNotNull())
            .filter(F.col("start_station_id").isin(stations))
            .filter(F.col("duration_min").isNull() | (F.col("duration_min") >= 0))
            .dropDuplicates(["trip_id"]))


def start_silver(spark, trigger="3 seconds"):
    stations = known_stations(spark)
    if not DeltaTable.isDeltaTable(spark, SILVER):      # table vide avec schema : permet de la lire en streaming
        spark.createDataFrame([], TRIPS_SCHEMA).write.format("delta").mode("append").save(SILVER)

    def merge_batch(batch: DataFrame, batch_id: int):
        rows = to_silver_rows(batch, stations)
        (DeltaTable.forPath(spark, SILVER).alias("s").merge(rows.alias("n"), "s.trip_id = n.trip_id")
         .whenNotMatchedInsertAll().execute())          # idempotent : un rejeu n'ajoute rien

    return (spark.readStream.format("delta").load(BRONZE).select("value")
            .writeStream.foreachBatch(merge_batch).option("checkpointLocation", f"{CKPT}/silver")
            .trigger(processingTime=trigger).queryName("silver").start())


# ---------------------------------------------------------------- GOLD
def start_gold(spark, trigger="5 seconds"):
    def refresh(batch: DataFrame, batch_id: int):
        silver = spark.read.format("delta").load(SILVER)
        kpi = (silver.withColumn("jour", F.to_date("start_time")).groupBy("city", "jour")
               .agg(*kpi_aggregations()).orderBy("city", "jour"))
        kpi.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_KPI)
        top = (silver.filter("status = 'completed'").groupBy("start_station_id")
               .agg(F.count("*").alias("departs"), F.round(F.sum("price_mad"), 2).alias("chiffre_affaires"))
               .orderBy(F.desc("departs"), "start_station_id"))
        top.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_TOP)

    return (spark.readStream.format("delta").load(SILVER).select("trip_id")
            .writeStream.foreachBatch(refresh).option("checkpointLocation", f"{CKPT}/gold")
            .trigger(processingTime=trigger).queryName("gold").start())


def wait_for_table(spark, path, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        if DeltaTable.isDeltaTable(spark, path):
            return
        time.sleep(2)
    raise TimeoutError(f"table Delta absente : {path}")


def start_all(spark):
    bronze = start_bronze(spark)
    wait_for_table(spark, BRONZE)          # Silver lit Bronze en streaming : la table doit exister
    silver = start_silver(spark)
    return [bronze, silver, start_gold(spark)]
