"""Schemas Spark explicites (sans inference) + regles de qualite, format du sujet Pedali.ma.

Importable depuis un notebook : sys.path.append("../data"); from pedali_schema import *
Voir docs/schema_donnees.md pour la description des champs.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# Un evenement JSON par changement d'etat d'un trajet.
TRIPS_SCHEMA = StructType([
    StructField("trip_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("vehicle_type", StringType(), False),     # BIKE | EBIKE | SCOOTER
    StructField("start_station", StringType(), False),
    StructField("end_station", StringType(), True),       # renseigne seulement pour ENDED
    StructField("city", StringType(), False),
    StructField("status", StringType(), False),           # RESERVED | UNLOCKED | ENDED | CANCELLED | FAULT_REPORTED
    StructField("duration_s", IntegerType(), True),       # ENDED seulement
    StructField("distance_km", DoubleType(), True),       # ENDED seulement
    StructField("battery_pct", IntegerType(), True),      # null pour BIKE
    StructField("price_mad", DoubleType(), True),         # ENDED seulement
    StructField("event_time", TimestampType(), False),    # ISO 8601 UTC ("...Z")
])

STATIONS_SCHEMA = StructType([
    StructField("station_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("district", StringType(), False),         # quartier
    StructField("zone_type", StringType(), False),        # gare, universite, plage, centre_affaires, residentiel, tourisme, commercial
    StructField("capacity", IntegerType(), False),        # nombre de bornes
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])


def read_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(TRIPS_SCHEMA).json(path)


def read_stations(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(STATIONS_SCHEMA).option("multiLine", True).json(path)


def clean_trips(trips: DataFrame, stations: DataFrame = None) -> DataFrame:
    """Un evenement = (trip_id, status) : les re-emissions at-least-once (doublons) sont supprimees."""
    return trips.dropDuplicates(["trip_id", "status"])


def quality_report(trips: DataFrame, stations: DataFrame) -> dict:
    """Nombre de lignes en violation, pour chaque regle."""
    known = [r[0] for r in stations.select("station_id").collect()]
    total = trips.count()
    distinct = trips.dropDuplicates(["trip_id", "status"]).count()
    return {
        "evenements": total,
        "doublons (meme trip_id + meme statut)": total - distinct,
        "station de depart inconnue": trips.filter(~F.col("start_station").isin(known)).count(),
        "ENDED sans duree/prix": trips.filter((F.col("status") == "ENDED") & (F.col("duration_s").isNull() | F.col("price_mad").isNull())).count(),
        "champs ENDED renseignes hors ENDED": trips.filter((F.col("status") != "ENDED") & F.col("end_station").isNotNull()).count(),
        "duree negative": trips.filter(F.col("duration_s") < 0).count(),
    }
