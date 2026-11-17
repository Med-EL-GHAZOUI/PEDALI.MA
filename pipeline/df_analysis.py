"""Partie 3 : Spark SQL et DataFrames sur trips_history.jsonl et stations.json (schema explicite, sans inference).

  1. chiffre d'affaires par ville : API DataFrame puis SQL (createOrReplaceTempView + spark.sql)
  2. jointure des trajets termines avec le referentiel stations (station de depart) : trajets et CA par type de zone et par ville
  3. UDF trip_band(duration_s) : court (< 10 min), moyen (10 a 30 min), long (> 30 min), puis version sans UDF (when/otherwise)
  4. ecriture du resultat nettoye en Parquet : s3a://lake/silver/trips_history/ partitionne par city
  5. explain(True) sur la jointure : plan logique, plan optimise Catalyst, strategie de jointure physique
"""
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, "/home/jovyan/work/data")
from pedali_schema import clean_trips, read_stations, read_trips

BASE = "s3a://lake/bronze/history"
SILVER_HISTORY = "s3a://lake/silver/trips_history"


def trip_band(duration_s):
    """UDF Python : court (< 10 min), moyen (10 a 30 min), long (> 30 min) ; None si la duree est inconnue."""
    if duration_s is None:
        return None
    return "court" if duration_s < 600 else "moyen" if duration_s <= 1800 else "long"


def trip_band_native(col: str = "duration_s"):
    """Meme regle sans UDF : expressions Spark natives (when/otherwise), optimisables par Catalyst."""
    c = F.col(col)
    return F.when(c.isNull(), F.lit(None)).when(c < 600, "court").when(c <= 1800, "moyen").otherwise("long")


def load(spark: SparkSession, base: str = BASE):
    stations = read_stations(spark, f"{base}/stations.json")
    trips = clean_trips(read_trips(spark, f"{base}/trips_history.jsonl"))
    return stations, trips


def analyses(spark: SparkSession, stations: DataFrame, trips: DataFrame) -> dict:
    trips.createOrReplaceTempView("trips")
    stations.createOrReplaceTempView("stations")
    ended = trips.filter("status = 'ENDED'")

    ca_df = (ended.groupBy("city").agg(F.round(F.sum("price_mad"), 2).alias("chiffre_affaires"), F.count("*").alias("trajets"))
             .orderBy("city"))
    ca_sql = spark.sql("""
        SELECT city, ROUND(SUM(price_mad), 2) AS chiffre_affaires, COUNT(*) AS trajets
        FROM trips WHERE status = 'ENDED' GROUP BY city ORDER BY city""")

    zone_df = (ended.join(stations.select(F.col("station_id").alias("start_station"), F.col("zone_type")), "start_station")
               .groupBy("city", "zone_type").agg(F.count("*").alias("trajets"), F.round(F.sum("price_mad"), 2).alias("chiffre_affaires"))
               .orderBy("city", F.desc("trajets")))
    zone_sql = spark.sql("""
        SELECT t.city, s.zone_type, COUNT(*) AS trajets, ROUND(SUM(t.price_mad), 2) AS chiffre_affaires
        FROM trips t JOIN stations s ON t.start_station = s.station_id
        WHERE t.status = 'ENDED' GROUP BY t.city, s.zone_type ORDER BY t.city, trajets DESC""")

    return {"ca_df": ca_df, "ca_sql": ca_sql, "zone_df": zone_df, "zone_sql": zone_sql, "ended": ended}


def bands(spark: SparkSession, ended: DataFrame) -> dict:
    """UDF vs expressions natives : memes resultats, temps mesures."""
    spark.sparkContext.addPyFile(__file__)          # l'UDF vit dans ce module : les executors doivent pouvoir l'importer
    spark.udf.register("trip_band", trip_band, StringType())
    udf_col = F.udf(trip_band, StringType())

    def timed(df, n=3):
        best, res = 1e9, None
        for _ in range(n):
            t = time.perf_counter()
            res = sorted((r["band"], r["trajets"]) for r in df.groupBy(F.col("band")).agg(F.count("*").alias("trajets")).collect())
            best = min(best, time.perf_counter() - t)
        return best, res

    ended = ended.cache()
    ended.count()
    t_udf, r_udf = timed(ended.withColumn("band", udf_col("duration_s")))
    t_native, r_native = timed(ended.withColumn("band", trip_band_native()))
    return {"udf": (t_udf, r_udf), "native": (t_native, r_native), "identiques": r_udf == r_native}


def write_silver_history(trips: DataFrame, path: str = SILVER_HISTORY) -> int:
    """Resultat nettoye (evenements dedoublonnes + trip_band), partitionne par ville."""
    out = trips.withColumn("trip_band", trip_band_native())
    out.write.mode("overwrite").partitionBy("city").parquet(path)
    return out.count()
