"""Schemas Spark explicites (Personne 2) + regles de qualite en DataFrame.

Importable depuis un notebook : sys.path.append("../data"); from pedali_schema import *
Voir docs/schema_donnees.md pour la description des champs.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

TRIPS_SCHEMA = StructType([
    StructField("trip_id", StringType(), False),
    StructField("user_id", StringType(), True),   # nullable a la lecture : les nulls sont une anomalie a filtrer
    StructField("bike_id", StringType(), False),
    StructField("start_station_id", StringType(), False),
    StructField("end_station_id", StringType(), True),
    StructField("city", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("duration_min", IntegerType(), True),
    StructField("price_mad", DoubleType(), False),
    StructField("status", StringType(), False),
])

STATIONS_SCHEMA = StructType([
    StructField("station_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("capacity", IntegerType(), False),
])


def read_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(TRIPS_SCHEMA).json(path)


def read_stations(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(STATIONS_SCHEMA).option("multiLine", True).json(path)


def clean_trips(trips: DataFrame, stations: DataFrame) -> DataFrame:
    """Regles de qualite : user_id non null, duree >= 0, station de depart connue, trip_id unique."""
    known = stations.select(F.col("station_id").alias("start_station_id"))
    return (trips
            .filter(F.col("user_id").isNotNull())
            .filter(F.col("duration_min").isNull() | (F.col("duration_min") >= 0))
            .join(known, "start_station_id", "left_semi")
            .dropDuplicates(["trip_id"]))


def quality_report(trips: DataFrame, stations: DataFrame) -> dict:
    """Nombre de lignes en violation, pour chaque regle."""
    known_ids = [r[0] for r in stations.select("station_id").collect()]
    dup = (trips.groupBy("trip_id").count().filter("count > 1")
           .agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0])
    return {
        "lignes": trips.count(),
        "doublons (trip_id repete)": dup,
        "user_id manquant": trips.filter(F.col("user_id").isNull()).count(),
        "station de depart inconnue": trips.filter(~F.col("start_station_id").isin(known_ids)).count(),
        "duree negative": trips.filter(F.col("duration_min") < 0).count(),
        "termine sans end_time": trips.filter((F.col("status") == "completed") & F.col("end_time").isNull()).count(),
    }
