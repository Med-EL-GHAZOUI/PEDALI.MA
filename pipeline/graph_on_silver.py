"""Partie 6 (Personne 1 + Personne 2) : GraphFrames branche sur la couche Silver (Delta, MinIO).

Reutilise les fonctions de la Personne 2 (graphframes/graph_analysis.py) : seule la source change
(`load_silver` lit s3a://lake/silver/trips au lieu des fichiers). Verifie que le resultat est identique
a l'analyse faite depuis les fichiers (compatibilite du schema Silver).
"""
import sys

sys.path.append("/home/jovyan/work/pipeline")
sys.path.append("/home/jovyan/work/graphframes")
sys.path.append("/home/jovyan/work/data")
from graphframes import GraphFrame
from pyspark.sql import functions as F

from pedali_spark import get_spark
from medallion import SILVER
import graph_analysis as ga
from pedali_schema import read_stations

spark = get_spark("pedali-graphframes-silver", delta=True)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("s3a://lake/checkpoints/graphframes")

stations = read_stations(spark, "file:///home/jovyan/work/data/stations.json")
silver = spark.read.format("delta").load(SILVER)
done = silver.filter((F.col("status") == "completed") & F.col("end_station_id").isNotNull()).cache()
g = ga.build_graph(stations, done)
print(f"Silver : {silver.count()} trajets valides, {done.count()} termines")
print(f"Graphe : {g.vertices.count()} sommets, {g.edges.count()} aretes")

print("\n== Corridors ==")
ga.corridors(g, 5).show()
print("== Allers-retours (>= 50 trajets) ==")
rt = ga.round_trips(g, 50)
rt.orderBy(F.desc("total")).show(5)
print("== Boucles a trois stations (>= 50) ==")
tri = ga.triangles(g, 50)
tri.orderBy(F.desc("total")).show(5)
print("== Solde net (sources / puits) ==")
roles = ga.sources_and_sinks(ga.net_balance(done, by_period=False), stations).orderBy("solde_net")
roles.show(12, truncate=False)

# --- compatibilite : meme resultat que l'analyse faite directement depuis les fichiers
from pedali_schema import clean_trips, read_trips
files = clean_trips(read_trips(spark, "file:///home/jovyan/work/data/trips_history.jsonl"), stations)
files = files.filter((F.col("status") == "completed") & F.col("end_station_id").isNotNull())
g2 = ga.build_graph(stations, files)
same_edges = (g.edges.exceptAll(g2.edges).count() == 0 and g2.edges.exceptAll(g.edges).count() == 0)
print("Aretes Silver == aretes fichiers :", same_edges)
print("RESULTAT :", "OK - GraphFrames compatible avec Silver" if same_edges else "ECART (Silver incomplet ?)")
spark.stop()
