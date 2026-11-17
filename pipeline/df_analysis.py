"""Partie 3 (Personne 1) : DataFrames, SQL, UDF, agregations et exports Parquet.

Les trajets nettoyes (regles de qualite de la Personne 2, data/pedali_schema.py) sont analyses en
DataFrame puis en SQL pur (memes resultats), avec une UDF de categorisation de duree.
Exports Parquet : s3a://lake/export/parquet/<table>/ (MinIO).
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, "/home/jovyan/work/data")
from pedali_schema import clean_trips, read_stations, read_trips

DATA = "file:///home/jovyan/work/data"
EXPORT = "s3a://lake/export/parquet"


def duration_bucket(minutes):
    """UDF : categorie de duree (None -> 'inconnue')."""
    if minutes is None:
        return "inconnue"
    return "courte (<10)" if minutes < 10 else "moyenne (10-29)" if minutes < 30 else "longue (>=30)"


def load(spark: SparkSession):
    stations = read_stations(spark, f"{DATA}/stations.json")
    trips = clean_trips(read_trips(spark, f"{DATA}/trips_history.jsonl"), stations)
    return stations, trips


def analyses(spark: SparkSession, stations: DataFrame, trips: DataFrame) -> dict:
    spark.sparkContext.addPyFile(__file__)   # l UDF vit dans ce module : les executors doivent pouvoir l importer
    spark.udf.register("duration_bucket", duration_bucket, StringType())
    bucket = F.udf(duration_bucket, StringType())
    trips.createOrReplaceTempView("trips")
    stations.createOrReplaceTempView("stations")
    done = trips.filter("status = 'completed'")

    return {
        "revenus_par_ville": (done.groupBy("city").agg(F.round(F.sum("price_mad"), 2).alias("chiffre_affaires"),
                                                      F.count("*").alias("trajets"),
                                                      F.round(F.avg("duration_min"), 2).alias("duree_moyenne_min"))
                              .orderBy("city")),
        "top5_stations": (done.join(stations.select(F.col("station_id").alias("start_station_id"), F.col("name").alias("station")),
                                    "start_station_id")
                          .groupBy("start_station_id", "station").agg(F.count("*").alias("departs"))
                          .orderBy(F.desc("departs"), "start_station_id").limit(5)),
        "repartition_duree_udf": (done.withColumn("categorie_duree", bucket("duration_min"))
                                  .groupBy("categorie_duree").agg(F.count("*").alias("trajets"))
                                  .orderBy("categorie_duree")),
        "statuts": trips.groupBy("status").agg(F.count("*").alias("trajets"),
                                               F.round(F.sum("price_mad"), 2).alias("chiffre_affaires")).orderBy("status"),
        "trajets_par_heure": (done.groupBy(F.hour("start_time").alias("heure")).agg(F.count("*").alias("trajets"))
                              .orderBy("heure")),
        # SQL pur : doit redonner les memes chiffres que la version DataFrame
        "sql_revenus_par_ville": spark.sql("""
            SELECT city, ROUND(SUM(price_mad), 2) AS chiffre_affaires, COUNT(*) AS trajets,
                   ROUND(AVG(duration_min), 2) AS duree_moyenne_min
            FROM trips WHERE status = 'completed' GROUP BY city ORDER BY city"""),
        "sql_top5_stations": spark.sql("""
            SELECT t.start_station_id, s.name AS station, COUNT(*) AS departs
            FROM trips t JOIN stations s ON t.start_station_id = s.station_id
            WHERE t.status = 'completed' GROUP BY t.start_station_id, s.name
            ORDER BY departs DESC, t.start_station_id LIMIT 5"""),
        "sql_udf": spark.sql("""
            SELECT duration_bucket(duration_min) AS categorie_duree, COUNT(*) AS trajets
            FROM trips WHERE status = 'completed' GROUP BY 1 ORDER BY 1"""),
    }


def export_parquet(trips: DataFrame, results: dict, base: str = EXPORT) -> list:
    written = []
    trips.write.mode("overwrite").partitionBy("city").parquet(f"{base}/trips_clean")
    written.append("trips_clean (partitionne par city)")
    for name in ("revenus_par_ville", "top5_stations", "repartition_duree_udf", "statuts", "trajets_par_heure"):
        results[name].coalesce(1).write.mode("overwrite").parquet(f"{base}/{name}")
        written.append(name)
    return written
