"""Partie 5 : pipeline de production Spark-Kafka (Bronze / Silver / Gold).

  Kafka trips --+--> BRONZE  s3a://lake/bronze/trips_raw/          JSON bruts (cle, valeur, partition, offset)
                |
                +--> nettoyage : watermark 10 min + dropDuplicatesWithinWatermark(["trip_id", "status"])
                     enrichissement : jointure stream-static avec stations.json (quartier, type de zone) + colonne trip_band
                     |--> SILVER  s3a://lake/silver/trips_clean/        Parquet partitionne par city et date
                     |--> fenetre tumbling 10 min par ville (trajets termines, CA, duree moyenne)
                     |       --> GOLD  s3a://lake/gold/kpi_city_10min/   Delta Lake
                     |--> fenetre sliding 10 min / pas 5 min par ville (taux de panne, taux d'annulation)
                     |       --> s3a://lake/gold/rates_city_sliding/     Delta Lake
                     |--> fenetre session (inactivite 5 min) par user_id (duree, trajets enchaines)
                             --> s3a://lake/gold/user_sessions/          Delta Lake

Chaque requete a son checkpoint (s3a://lake/checkpoints/p5_*) : apres un arret, la reprise continue aux offsets
Kafka enregistres dans le checkpoint (source rejouable), avec l'etat de dedoublonnage et de fenetres.

Usage (Jupyter / cluster) : python pipeline/medallion.py --seconds 300
"""
import argparse
import json
import sys
import time

sys.path.insert(0, "/home/jovyan/work/data")
sys.path.insert(0, "/home/jovyan/work/pipeline")
import part4_streaming as p4
from df_analysis import trip_band_native
from pedali_schema import read_stations
from pedali_spark import get_spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LAKE = "s3a://lake"
SILVER = f"{LAKE}/silver/trips_clean"
GOLD_10MIN = f"{LAKE}/gold/kpi_city_10min"
GOLD_SLIDING = f"{LAKE}/gold/rates_city_sliding"
GOLD_SESSIONS = f"{LAKE}/gold/user_sessions"
CKPT = f"{LAKE}/checkpoints"
STATIONS = f"{LAKE}/bronze/history/stations.json"
WATERMARK = "10 minutes"
TRIGGER = "5 seconds"


def cleaned_stream(spark: SparkSession, topic: str = p4.TOPIC, max_offsets: int = 1000) -> DataFrame:
    """Flux nettoye et enrichi (utilise par toutes les requetes ci-dessous)."""
    events = p4.parse_events(p4.read_kafka(spark, topic=topic, max_offsets=max_offsets)).filter("trip_id IS NOT NULL")
    deduped = (events.withWatermark("event_time", WATERMARK)
               .dropDuplicatesWithinWatermark(["trip_id", "status"]))             # re-emissions at-least-once
    stations = read_stations(spark, STATIONS).select(F.col("station_id").alias("start_station"), "district", "zone_type")
    enriched = deduped.join(F.broadcast(stations), "start_station", "left")        # jointure stream-static
    return (enriched.withColumn("trip_band", trip_band_native("duration_s"))
            .withColumn("date", F.to_date("event_time")))


def start_silver(clean: DataFrame):
    return (clean.writeStream.format("parquet").outputMode("append").partitionBy("city", "date")
            .option("path", SILVER).option("checkpointLocation", f"{CKPT}/p5_silver")
            .trigger(processingTime=TRIGGER).queryName("silver").start())


def tumbling_kpi(clean: DataFrame) -> DataFrame:
    """Fenetre tumbling de 10 minutes par ville : trajets termines, chiffre d'affaires, duree moyenne."""
    return (clean.groupBy(F.window("event_time", "10 minutes").alias("w"), "city")
            .agg(F.sum(F.when(F.col("status") == "ENDED", 1).otherwise(0)).alias("trajets_termines"),
                 F.round(F.sum("price_mad"), 2).alias("chiffre_affaires"),
                 F.round(F.avg("duration_s"), 1).alias("duree_moyenne_s"))
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "city",
                    "trajets_termines", "chiffre_affaires", "duree_moyenne_s"))


def sliding_rates(clean: DataFrame) -> DataFrame:
    """Fenetre glissante de 10 minutes, pas de 5 minutes, par ville : taux de panne et taux d'annulation."""
    return (clean.groupBy(F.window("event_time", "10 minutes", "5 minutes").alias("w"), "city")
            .agg(F.count("*").alias("evenements"),
                 F.sum(F.when(F.col("status") == "FAULT_REPORTED", 1).otherwise(0)).alias("pannes"),
                 F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("annulations"))
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "city", "evenements",
                    F.round(F.col("pannes") / F.col("evenements"), 4).alias("taux_panne"),
                    F.round(F.col("annulations") / F.col("evenements"), 4).alias("taux_annulation")))


def user_sessions(clean: DataFrame) -> DataFrame:
    """Fenetre session (inactivite de 5 minutes) par utilisateur : duree de la session et trajets enchaines."""
    return (clean.groupBy("user_id", F.session_window("event_time", "5 minutes").alias("s"))
            .agg((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))).alias("duree_session_s"),
                 F.sum(F.when(F.col("status") == "RESERVED", 1).otherwise(0)).alias("trajets_enchaines"))
            .select("user_id", F.col("s.start").alias("session_start"), F.col("s.end").alias("session_end"),
                    "duree_session_s", "trajets_enchaines"))


def start_delta(df: DataFrame, path: str, name: str, checkpoint: str):
    return (df.writeStream.format("delta").outputMode("append").option("path", path)
            .option("checkpointLocation", f"{CKPT}/{checkpoint}").trigger(processingTime=TRIGGER).queryName(name).start())


ALL_QUERIES = ("bronze", "silver", "gold_10min", "gold_sliding", "gold_sessions")


def start_all(spark: SparkSession, topic: str = p4.TOPIC, max_offsets: int = 1000, which: tuple = ALL_QUERIES) -> list:
    """Demarre les requetes demandees (par defaut les cinq). Chaque requete relit Kafka avec son propre checkpoint."""
    raw = p4.read_kafka(spark, topic=topic, max_offsets=max_offsets)
    clean = cleaned_stream(spark, topic=topic, max_offsets=max_offsets)
    builders = {
        "bronze": lambda: p4.start_bronze_raw(raw),
        "silver": lambda: start_silver(clean),
        "gold_10min": lambda: start_delta(tumbling_kpi(clean), GOLD_10MIN, "gold_10min", "p5_gold_10min"),
        "gold_sliding": lambda: start_delta(sliding_rates(clean), GOLD_SLIDING, "gold_sliding", "p5_gold_sliding"),
        "gold_sessions": lambda: start_delta(user_sessions(clean), GOLD_SESSIONS, "gold_sessions", "p5_gold_sessions"),
    }
    return [builders[name]() for name in which]


def _offsets_read(q):
    """Somme des offsets de fin lus par une requete (None tant qu'aucun micro-batch n'a eu lieu)."""
    p = q.lastProgress
    if not p or not p.get("sources"):
        return None
    eo = p["sources"][0]["endOffset"]
    eo = json.loads(eo) if isinstance(eo, str) else eo
    return sum(v for part in eo.values() for v in part.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--queries", default=",".join(ALL_QUERIES), help="requetes a demarrer (separees par des virgules)")
    ap.add_argument("--until-total", type=int, default=0,
                    help="s'arreter des que chaque requete a lu ce nombre d'offsets Kafka (0 = jusqu'a --seconds)")
    args = ap.parse_args()
    spark = get_spark("pedali-part5-pipeline", delta=True, spark__sql__shuffle__partitions="3")
    spark.sparkContext.setLogLevel("ERROR")
    queries = start_all(spark, which=tuple(args.queries.split(",")))
    end = time.time() + args.seconds
    while time.time() < end:
        time.sleep(15)
        if args.until_total and all(_offsets_read(q) == args.until_total and not q.status["isDataAvailable"] for q in queries):
            time.sleep(20)                  # dernier micro-batch (vide) : emission des fenetres fermees par le watermark
            break
        for q in queries:
            p = q.lastProgress
            if p:
                print(f"### {q.name:14s} batch {p['batchId']:>3} : {p['numInputRows']:>5} lignes, "
                      f"{p['processedRowsPerSecond']:.0f} l/s, watermark {p.get('eventTime', {}).get('watermark')}", flush=True)
    for q in queries:
        q.stop()
    spark.stop()


if __name__ == "__main__":
    main()
