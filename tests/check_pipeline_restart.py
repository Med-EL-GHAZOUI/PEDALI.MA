"""Partie 5 (Personne 1) : test d'arret / redemarrage du pipeline Kafka -> Bronze -> Silver -> Gold.

Deroulement (dans le conteneur Jupyter) :
  1. topic `trips` recree, lake nettoye ; 5000 messages produits ; pipeline demarre, Bronze atteint 5000 ;
  2. ARRET des 3 requetes + de la SparkSession ;
  3. 5000 messages supplementaires produits PENDANT l'arret ;
  4. REDEMARRAGE avec les memes checkpoints ;
  5. verifications : Bronze = 10000 lignes sans doublon (partition, offset), Silver et Gold == reference Python
     independante calculee sur les 10000 premieres lignes du fichier ; DESCRIBE HISTORY de Bronze.
Usage : python tests/check_pipeline_restart.py   (sortie archivee dans reports/pipeline_restart_output.txt)
"""
import json
import subprocess
import sys
import time

sys.path.append("/home/jovyan/work/pipeline")
from pedali_spark import get_spark
import medallion as md
from pyspark.sql import functions as F

DATA = "/home/jovyan/work/data"
N1, N2 = 5000, 5000


def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def reset_world():
    """Topic recree + lake nettoye (via Hadoop FileSystem)."""
    sh(["python", "-c", "from kafka.admin import KafkaAdminClient as A; A(bootstrap_servers='kafka:9092').delete_topics(['trips'])"])
    time.sleep(6)
    out = sh(["python", "-c", "from kafka.admin import KafkaAdminClient as A, NewTopic as T;"
              "A(bootstrap_servers='kafka:9092').create_topics([T('trips',3,1)])"])
    print("topic trips recree (3 partitions)", out.stderr[-200:])
    spark = get_spark("pedali-reset", delta=True)
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI("s3a://lake"), spark._jsc.hadoopConfiguration())
    for p in ("bronze", "silver", "gold", "checkpoints"):
        fs.delete(jvm.org.apache.hadoop.fs.Path(f"s3a://lake/{p}"), True)
    spark.stop()


def produce(skip, limit):
    out = sh(["python", "/home/jovyan/work/producer/producer.py", "--bootstrap", "kafka:9092",
              "--skip", str(skip), "--limit", str(limit)])
    print(out.stdout.strip(), out.stderr[-300:])


def wait_for(spark, path, expected, timeout=300):
    end = time.time() + timeout
    n = -1
    while time.time() < end:
        try:
            n = spark.read.format("delta").load(path).count()
        except Exception:
            n = -1
        if n >= expected:
            return n
        time.sleep(5)
    return n


def reference(n_lines):
    stations = {s["station_id"] for s in json.load(open(f"{DATA}/stations.json", encoding="utf-8"))}
    seen, clean = set(), []
    for line in open(f"{DATA}/trips_history.jsonl", encoding="utf-8").read().splitlines()[:n_lines]:
        t = json.loads(line)
        if (not t["user_id"] or t["start_station_id"] not in stations
                or (t["duration_min"] is not None and t["duration_min"] < 0) or t["trip_id"] in seen):
            continue
        seen.add(t["trip_id"])
        clean.append(t)
    done = [t for t in clean if t["status"] == "completed"]
    return {"silver": len(clean), "revenu": round(sum(t["price_mad"] for t in done), 2), "termines": len(done)}


def main():
    reset_world()

    # ---- phase 1
    produce(0, N1)
    spark = get_spark("pedali-pipeline-phase1", delta=True)
    spark.sparkContext.setLogLevel("ERROR")
    queries = md.start_all(spark)
    b = wait_for(spark, md.BRONZE, N1)
    s = wait_for(spark, md.SILVER, reference(N1)["silver"])
    time.sleep(12)  # laisse l'or se rafraichir
    print(f"[phase 1] Bronze={b} Silver={s}")
    ids_before = {q.name: q.id for q in queries}
    for q in queries:
        q.stop()
    spark.stop()
    print("[phase 1] ARRET des requetes et de la SparkSession")

    # ---- arret : production pendant l'indisponibilite du pipeline
    produce(N1, N2)

    # ---- phase 2
    spark = get_spark("pedali-pipeline-phase2", delta=True)
    spark.sparkContext.setLogLevel("ERROR")
    queries = md.start_all(spark)
    ref = reference(N1 + N2)
    b = wait_for(spark, md.BRONZE, N1 + N2)
    s = wait_for(spark, md.SILVER, ref["silver"])
    time.sleep(15)
    for q in queries:
        print(f"[phase 2] requete {q.name}: meme queryId qu'avant l'arret = {q.id == ids_before[q.name]}")

    bronze = spark.read.format("delta").load(md.BRONZE)
    bronze_n = bronze.count()
    distinct_offsets = bronze.select("partition", "offset").distinct().count()
    silver = spark.read.format("delta").load(md.SILVER)
    silver_n, silver_distinct = silver.count(), silver.select("trip_id").distinct().count()
    gold = spark.read.format("delta").load(md.GOLD_KPI)
    g = gold.agg(F.sum("trajets_termines"), F.round(F.sum("chiffre_affaires"), 2)).first()
    print(f"[phase 2] Bronze={bronze_n} (offsets distincts={distinct_offsets})")
    print(f"[phase 2] Silver={silver_n} (trip_id distincts={silver_distinct}) ; reference={ref['silver']}")
    print(f"[phase 2] Gold : trajets termines={g[0]} chiffre d'affaires={g[1]} ; reference={ref['termines']} / {ref['revenu']}")
    print("\nGold kpi_city_day (extrait) :")
    gold.orderBy("city", "jour").show(6, truncate=False)
    print("DESCRIBE HISTORY bronze :")
    (spark.sql(f"DESCRIBE HISTORY delta.`{md.BRONZE}`")
     .select("version", "timestamp", "operation", F.col("operationMetrics.numOutputRows").alias("lignes"))
     .orderBy("version").show(30, truncate=False))

    ok = (bronze_n == N1 + N2 == distinct_offsets and silver_n == silver_distinct == ref["silver"]
          and g[0] == ref["termines"] and abs(g[1] - ref["revenu"]) < 0.01)
    print("RESULTAT :", "OK - aucune perte, aucun doublon apres redemarrage" if ok else "ECHEC")
    for q in queries:
        q.stop()
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
