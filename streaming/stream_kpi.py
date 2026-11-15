"""Partie 4 - Structured Streaming : watermark, evenements en retard, doublons, modes de sortie (Personne 2).

Le coeur (`kpi_stream`) ne depend pas de la source : il recoit un DataFrame streaming deja parse
(schema `TRIPS_SCHEMA`). Sources fournies :
  - `read_kafka`  : topic `trips` (cluster Docker, Partie 4 de la Personne 1 : lecture Kafka + parsing JSON)
  - `read_socket` : lignes JSON sur un socket TCP (test local sans Kafka, voir tests/check_streaming.py)
  - `read_files`  : dossier de fichiers JSONL (Linux / cluster)

Regles :
  - temps evenement = `start_time` ; watermark = 10 min (un evenement plus vieux que
    max(start_time) - 10 min est ignore par l'agregation) ;
  - qualite : user_id non nul, duree >= 0, station de depart connue ;
  - doublons : `dropDuplicatesWithinWatermark(["trip_id"])` (reemission Kafka, producteur "at-least-once") ;
  - fenetres fixes de 15 min sur `start_time`, memes KPI que la version batch (data/pedali_kpi.py).

Modes de sortie (voir docs/partie4_streaming.md) :
  - update   : emet les fenetres modifiees a chaque micro-batch (tableau de bord temps reel) ;
  - append   : emet chaque fenetre UNE fois, quand le watermark la ferme (resultat final, vers un sink fichier / Delta) ;
  - complete : re-emet tout le tableau a chaque batch (watermark sans effet sur l'etat ; petit volume seulement).

Usage CLI (cluster) :
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/stream_kpi.py \
        --source kafka --servers kafka:9092 --mode update
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "data"))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pedali_kpi import kpi_aggregations
from pedali_schema import TRIPS_SCHEMA, read_stations

WATERMARK = "10 minutes"
WINDOW = "15 minutes"


def parse_json_lines(df: DataFrame, column: str = "value") -> DataFrame:
    """Colonne texte JSON -> colonnes du schema ; les messages illisibles (struct nul) sont ecartes."""
    return (df.select(F.from_json(F.col(column).cast("string"), TRIPS_SCHEMA).alias("t"))
            .select("t.*").filter(F.col("trip_id").isNotNull()))


def read_kafka(spark: SparkSession, servers: str, topic: str = "trips", starting_offsets: str = "earliest") -> DataFrame:
    """Lit le topic Kafka et parse la valeur JSON."""
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", servers)
           .option("subscribe", topic)
           .option("startingOffsets", starting_offsets)
           .load())
    return parse_json_lines(raw)


def read_socket(spark: SparkSession, host: str, port: int) -> DataFrame:
    """Une ligne JSON par message sur un socket TCP : source de test sans Kafka ni acces Hadoop
    (la source fichiers de Spark exige les bibliotheques natives Hadoop sous Windows)."""
    raw = spark.readStream.format("socket").option("host", host).option("port", port).load()
    return parse_json_lines(raw)


def read_files(spark: SparkSession, directory: str, max_files_per_trigger: int = 1) -> DataFrame:
    """Dossier de fichiers JSONL : chaque fichier depose joue le role d'un lot de messages (Linux / cluster)."""
    return (spark.readStream.schema(TRIPS_SCHEMA)
            .option("maxFilesPerTrigger", max_files_per_trigger).json(directory))


def kpi_stream(trips: DataFrame, known_stations: list, watermark: str = WATERMARK, window: str = WINDOW) -> DataFrame:
    """KPI par fenetre de temps evenement, avec nettoyage, dedoublonnage et watermark."""
    cleaned = (trips
               .withWatermark("start_time", watermark)
               .filter(F.col("user_id").isNotNull())
               .filter(F.col("duration_min").isNull() | (F.col("duration_min") >= 0))
               .filter(F.col("start_station_id").isin(known_stations))
               .dropDuplicatesWithinWatermark(["trip_id"]))
    return (cleaned.groupBy(F.window("start_time", window).alias("fenetre"))
            .agg(*kpi_aggregations())
            .select(F.col("fenetre.start").alias("debut"), F.col("fenetre.end").alias("fin"),
                    "trajets_valides", "trajets_termines", "chiffre_affaires", "duree_moyenne_min",
                    "taux_panne", "taux_annulation"))


def known_station_ids(spark: SparkSession) -> list:
    return [r[0] for r in read_stations(spark, str(ROOT / "data/stations.json")).select("station_id").collect()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--servers", default="kafka:9092")
    ap.add_argument("--topic", default="trips")
    ap.add_argument("--dir", default=str(ROOT / "data/stream_in"), help="dossier JSONL (source files)")
    ap.add_argument("--mode", choices=["update", "append", "complete"], default="update")
    ap.add_argument("--checkpoint", default=os.environ.get("CHECKPOINT", "/tmp/pedali-ckpt-kpi"))
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("pedali-stream-kpi")
             .master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    src = read_kafka(spark, args.servers, args.topic) if args.source == "kafka" else read_files(spark, args.dir)
    out = kpi_stream(src, known_station_ids(spark))
    q = (out.writeStream.outputMode(args.mode).format("console").option("truncate", False)
         .option("checkpointLocation", args.checkpoint).start())
    q.awaitTermination()


if __name__ == "__main__":
    main()
