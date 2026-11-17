"""Partie 4 (Personne 1) : lecture Kafka (Structured Streaming), parsing JSON et sinks.

  Kafka `trips` --> from_json(TRIPS_SCHEMA) --+--> sink fichiers Parquet (s3a://lake/stream/trips_parsed)
                                              +--> sink console : KPI par fenetre (coeur de la Personne 2)

Les messages illisibles sont comptes a part (colonne `value` brute conservee) au lieu d'etre perdus.
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/home/jovyan/work/data")
sys.path.insert(0, "/home/jovyan/work/streaming")
from pedali_schema import TRIPS_SCHEMA
from stream_kpi import kpi_stream, known_station_ids

KAFKA = "kafka:9092"


def read_trips_stream(spark: SparkSession, servers: str = KAFKA, topic: str = "trips",
                      starting_offsets: str = "earliest", max_offsets: int = 2000) -> DataFrame:
    """Brut Kafka + metadonnees (partition, offset, horodatage Kafka) + JSON parse (null si illisible)."""
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", servers).option("subscribe", topic)
           .option("startingOffsets", starting_offsets)
           .option("maxOffsetsPerTrigger", max_offsets)     # limite la taille d'un micro-batch
           .option("failOnDataLoss", "false").load())
    return (raw.select(F.col("key").cast("string").alias("cle"), F.col("value").cast("string").alias("value"),
                       "partition", "offset", F.col("timestamp").alias("kafka_ts"))
            .withColumn("t", F.from_json("value", TRIPS_SCHEMA)))


def parsed_sink(stream: DataFrame, path: str, checkpoint: str, trigger: str = "5 seconds"):
    """Ecriture Parquet des messages valides (micro-batchs en mode append)."""
    ok = stream.filter(F.col("t.trip_id").isNotNull()).select("t.*", "partition", "offset", "kafka_ts")
    return (ok.writeStream.format("parquet").outputMode("append").option("path", path)
            .option("checkpointLocation", checkpoint).trigger(processingTime=trigger).queryName("parsed_parquet").start())


def kpi_console_sink(spark: SparkSession, stream: DataFrame, checkpoint: str, trigger: str = "5 seconds"):
    trips = stream.filter(F.col("t.trip_id").isNotNull()).select("t.*")
    out = kpi_stream(trips, known_station_ids_local(spark))
    return (out.writeStream.format("console").outputMode("update").option("truncate", False)
            .option("checkpointLocation", checkpoint).trigger(processingTime=trigger).queryName("kpi_console").start())


def known_station_ids_local(spark: SparkSession) -> list:
    from pedali_schema import read_stations
    return [r[0] for r in read_stations(spark, "file:///home/jovyan/work/data/stations.json").select("station_id").collect()]
