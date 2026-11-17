"""Partie 2 (Personne 1) : calculs RDD sur trips_history.jsonl.

Revenus par ville, duree moyenne, top 5 des stations de depart (trajets termines, apres nettoyage :
trip_id unique, user_id non nul, station connue, duree >= 0 -- regles de docs/schema_donnees.md).
Usage (dans Jupyter / le cluster) : python pipeline/rdd_analysis.py
"""
import json
import sys

DATA = "/home/jovyan/work/data"   # monte sur Jupyter ET sur les Workers (docker-compose.yml)


def parse(line):
    return json.loads(line)


def run(spark):
    sc = spark.sparkContext
    sc.addPyFile(__file__)   # les fonctions de ce module sont envoyees aux executors
    stations = {s["station_id"] for s in json.load(open(f"{DATA}/stations.json", encoding="utf-8"))}
    known = sc.broadcast(stations)

    raw = sc.textFile(f"file://{DATA}/trips_history.jsonl").filter(lambda l: l.strip()).map(parse)
    valid = (raw
             .filter(lambda t: t["user_id"] and t["start_station_id"] in known.value)
             .filter(lambda t: t["duration_min"] is None or t["duration_min"] >= 0)
             .map(lambda t: (t["trip_id"], t))
             .reduceByKey(lambda a, b: a)          # dedoublonnage sur trip_id
             .values())
    done = valid.filter(lambda t: t["status"] == "completed").cache()

    revenus = dict(done.map(lambda t: (t["city"], t["price_mad"])).reduceByKey(lambda a, b: a + b).collect())
    somme, nb = done.map(lambda t: (t["duration_min"], 1)).reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    top5 = (done.map(lambda t: (t["start_station_id"], 1)).reduceByKey(lambda a, b: a + b)
            .takeOrdered(5, key=lambda kv: (-kv[1], kv[0])))
    return {"lignes_brutes": raw.count(), "trajets_termines": nb,
            "revenus_par_ville": {k: round(v, 2) for k, v in sorted(revenus.items())},
            "duree_moyenne_min": round(somme / nb, 2), "top5_stations": top5,
            "lineage": done.toDebugString().decode()}


if __name__ == "__main__":
    sys.path.append("/home/jovyan/work/pipeline")
    from pedali_spark import get_spark
    spark = get_spark("pedali-02-rdd")
    res = run(spark)
    for k, v in res.items():
        print(k, ":", v)
    spark.stop()
