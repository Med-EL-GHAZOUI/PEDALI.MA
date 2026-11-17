"""Lance les calculs des Parties 2 et 3 puis garde la SparkSession ouverte pour consulter la Spark UI
(http://localhost:4040 : Jobs, Stages, DAG, onglet SQL) et faire les captures demandees.

Usage (Jupyter) : python tests/ui_demo.py [secondes_d_attente]
"""
import sys
import time

sys.path.append("/home/jovyan/work/pipeline")
sys.path.append("/home/jovyan/work/data")
from pedali_spark import get_spark
import df_analysis as da
import rdd_analysis
from pyspark.sql import functions as F

wait = int(sys.argv[1]) if len(sys.argv) > 1 else 600
spark = get_spark("pedali-ui-demo")
spark.sparkContext.setLogLevel("ERROR")

res = rdd_analysis.run(spark)                       # Partie 2 : jobs RDD (shuffles, stages sautes)
print("RDD ok :", res["chiffre_affaires_par_ville"], flush=True)

stations, trips = da.load(spark)                    # Partie 3 : jointure + agregation (onglet SQL)
ended = trips.filter("status = 'ENDED'")
s = stations.select(F.col("station_id").alias("start_station"), "zone_type")
q = (ended.join(s, "start_station").groupBy("city", "zone_type")
     .agg(F.count("*").alias("trajets"), F.round(F.sum("price_mad"), 2).alias("ca")))
q.show(5)
print("UI prete :", spark.sparkContext.uiWebUrl, flush=True)
time.sleep(wait)
spark.stop()
