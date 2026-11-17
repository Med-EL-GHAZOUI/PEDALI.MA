"""Partie 2 : traitement batch avec SparkCore (RDD) sur trips_history.jsonl.

Etapes du sujet : textFile + json.loads -> filter(ENDED) -> map/reduceByKey (CA par ville) ->
duree moyenne par ville et type de vehicule (aggregateByKey : somme et nombre en un seul passage) ->
top 5 des stations de depart (takeOrdered) -> lineage (toDebugString).

Le fichier est lu dans MinIO (s3a://lake/bronze/history/, voir scripts/load_history.sh) ; a defaut, sur le disque monte.
Les evenements dupliques (meme trip_id et meme statut) sont supprimes par un reduceByKey avant les calculs.

Usage (dans Jupyter / le cluster) : python pipeline/rdd_analysis.py
"""
import json
import sys

HISTORY = "s3a://lake/bronze/history/trips_history.jsonl"
LOCAL = "file:///home/jovyan/work/data/trips_history.jsonl"


def run(spark, path: str = HISTORY) -> dict:
    sc = spark.sparkContext
    raw = sc.textFile(path).filter(lambda l: l.strip()).map(json.loads)          # transformations (lazy)

    # dedoublonnage : cle (trip_id, status) -> on garde un seul evenement (shuffle)
    events = raw.map(lambda e: ((e["trip_id"], e["status"]), e)).reduceByKey(lambda a, b: a).values()
    ended = events.filter(lambda e: e["status"] == "ENDED").cache()

    revenue = dict(ended.map(lambda e: (e["city"], e["price_mad"])).reduceByKey(lambda a, b: a + b).collect())   # action

    avg_dur = (ended.map(lambda e: ((e["city"], e["vehicle_type"]), e["duration_s"]))
               .aggregateByKey((0, 0), lambda acc, d: (acc[0] + d, acc[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))
               .mapValues(lambda s: round(s[0] / s[1], 1)).collectAsMap())                                        # action

    top5 = (ended.map(lambda e: (e["start_station"], 1)).reduceByKey(lambda a, b: a + b)
            .takeOrdered(5, key=lambda kv: (-kv[1], kv[0])))                                                      # action

    return {
        "evenements_bruts": raw.count(),
        "evenements_apres_dedoublonnage": events.count(),
        "trajets_termines": ended.count(),
        "chiffre_affaires_par_ville": {k: round(v, 2) for k, v in sorted(revenue.items())},
        "duree_moyenne_s_par_ville_type": {f"{c}/{t}": v for (c, t), v in sorted(avg_dur.items())},
        "top5_stations_depart": top5,
        "lineage": ended.toDebugString().decode(),
    }


if __name__ == "__main__":
    sys.path.append("/home/jovyan/work/pipeline")
    from pedali_spark import get_spark
    spark = get_spark("pedali-02-rdd")
    for k, v in run(spark).items():
        print(k, ":", v)
    spark.stop()
