"""Partie 5 : preuves sur les couches produites (Silver, Gold Delta) : comptages, extraits, DESCRIBE HISTORY.

Usage (Jupyter) : python tests/pipeline_evidence.py
"""
import sys

sys.path.insert(0, "/home/jovyan/work/pipeline")
sys.path.insert(0, "/home/jovyan/work/data")
import medallion as md
from pedali_spark import get_spark
from pyspark.sql import functions as F

spark = get_spark("pedali-pipeline-evidence", delta=True, spark__sql__shuffle__partitions="3")
spark.sparkContext.setLogLevel("ERROR")

silver = spark.read.parquet(md.SILVER)
print(f"== SILVER {md.SILVER} : {silver.count()} lignes, doublons (trip_id, status) = "
      f"{silver.count() - silver.select('trip_id', 'status').distinct().count()}")
silver.printSchema()
silver.groupBy("city").count().orderBy("city").show()
silver.select("trip_id", "status", "city", "district", "zone_type", "duration_s", "trip_band", "event_time").filter("status = 'ENDED'").show(5, truncate=False)

for name, path, order in [("GOLD kpi_city_10min (fenetre tumbling 10 min)", md.GOLD_10MIN, ["window_start", "city"]),
                          ("GOLD rates_city_sliding (fenetre 10 min, pas 5 min)", md.GOLD_SLIDING, ["window_start", "city"]),
                          ("GOLD user_sessions (session, inactivite 5 min)", md.GOLD_SESSIONS, ["session_start", "user_id"])]:
    df = spark.read.format("delta").load(path)
    print(f"\n== {name} : {df.count()} lignes")
    df.orderBy(*order).show(6, truncate=False)

gold = spark.read.format("delta").load(md.GOLD_10MIN)
print("== Gold tumbling : totaux par ville")
gold.groupBy("city").agg(F.sum("trajets_termines").alias("trajets_termines"), F.round(F.sum("chiffre_affaires"), 2).alias("chiffre_affaires")).orderBy("city").show()
print("== Sessions : repartition du nombre de trajets enchaines")
spark.read.format("delta").load(md.GOLD_SESSIONS).groupBy("trajets_enchaines").count().orderBy("trajets_enchaines").show(8)

print("== DESCRIBE HISTORY de la table Delta Gold kpi_city_10min")
spark.sql(f"DESCRIBE HISTORY delta.`{md.GOLD_10MIN}`").select("version", "timestamp", "operation", "operationMetrics").orderBy(F.desc("version")).show(8, truncate=70)
spark.stop()
