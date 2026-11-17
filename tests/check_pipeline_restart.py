"""Partie 5 : reprise du pipeline apres arret, grace aux checkpoints (aucune perte, aucun doublon).

  phase 1 : le producteur envoie PHASE1 evenements ; le pipeline (Bronze, Silver, Gold) les traite ; arret propre
  phase 2 : pipeline ARRETE, le producteur continue pendant ~2 minutes (PHASE2 evenements a RATE evt/s)
  phase 3 : redemarrage avec les memes checkpoints ; le pipeline reprend aux offsets du checkpoint

Verifications : Bronze == offsets Kafka, offsets du checkpoint == fin du journal Kafka, aucun doublon en Bronze ni en
Silver, meme queryId avant/apres, Silver ne perd aucune ligne deja ecrite. Sortie : reports/pipeline_restart_output.txt.

Usage (Jupyter) : python tests/check_pipeline_restart.py [--phase1 10000] [--phase2 6000] [--rate 50]
"""
import argparse
import json
import subprocess
import sys
import time

sys.path.insert(0, "/home/jovyan/work/pipeline")
sys.path.insert(0, "/home/jovyan/work/data")
import medallion as md
import part4_streaming as p4
from pedali_spark import get_spark
from pyspark.sql import functions as F

PRODUCER = ["python", "/home/jovyan/work/producer/producer.py", "--mode", "kafka", "--bootstrap", "kafka:9092", "--n", "50000"]


def produce(skip: int, limit: int, rate: float):
    print(f"[producteur] evenements {skip}..{skip + limit} a {rate:.0f} evt/s", flush=True)
    subprocess.run(PRODUCER + ["--skip", str(skip), "--limit", str(limit), "--rate", str(rate)], check=True, capture_output=True)


def kafka_end_offsets(spark) -> dict:
    """Fin du journal Kafka par partition (lecture batch)."""
    df = (spark.read.format("kafka").option("kafka.bootstrap.servers", p4.KAFKA).option("subscribe", p4.TOPIC)
          .option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
    return {r.partition: r["max_off"] + 1 for r in df.groupBy("partition").agg(F.max("offset").alias("max_off")).collect()}


def checkpoint_offsets(spark, ckpt: str) -> dict:
    """Offsets de fin du dernier micro-batch valide du checkpoint (dossier offsets/<batchId>)."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{ckpt}/offsets")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    batches = sorted(int(s.getPath().getName()) for s in fs.listStatus(path) if s.getPath().getName().isdigit())
    last = batches[-1]
    lines = [r.value for r in spark.read.text(f"{ckpt}/offsets/{last}").collect()]
    offsets = json.loads(lines[-1])
    topic = next(iter(offsets))
    return {"batch": last, "offsets": {int(k): v for k, v in offsets[topic].items()}}


def query_id(spark, ckpt: str) -> str:
    return json.loads(spark.read.text(f"{ckpt}/metadata").first().value)["id"]


BRONZE_SCHEMA = ("key string, value string, topic string, partition int, offset long, kafka_ts timestamp")


def counts(spark) -> dict:
    """Comptages Bronze / Silver ; 0 tant que le dossier n'existe pas encore ou ne contient aucun fichier."""
    try:
        bronze = spark.read.schema(BRONZE_SCHEMA).json(p4.BRONZE_RAW)
        n_b, d_b = bronze.count(), bronze.select("partition", "offset").distinct().count()
    except Exception:
        n_b = d_b = 0
    try:
        silver = spark.read.parquet(md.SILVER)
        n_s, d_s = silver.count(), silver.select("trip_id", "status").distinct().count()
    except Exception:
        n_s = d_s = 0
    return {"bronze": n_b, "bronze_distincts": d_b, "silver": n_s, "silver_distincts": d_s}


def end_offsets_sum(q):
    p = q.lastProgress
    if not p or not p.get("sources"):
        return None
    eo = p["sources"][0]["endOffset"]
    eo = json.loads(eo) if isinstance(eo, str) else eo
    return sum(v for part in eo.values() for v in part.values())


def wait_caught_up(queries, total: int, what: str, timeout: int = 900):
    """Attend que toutes les requetes aient lu `total` offsets et n'aient plus de donnees a traiter.
    Le suivi passe par lastProgress (aucun job Spark supplementaire qui concurrencerait les requetes sur 4 coeurs)."""
    end = time.time() + timeout
    while time.time() < end:
        sums = {q.name: end_offsets_sum(q) for q in queries}
        idle = all(not q.status["isDataAvailable"] and not q.status["isTriggerActive"] for q in queries)
        print(f"   ... {what} : offsets lus {sums} / {total}", flush=True)
        if idle and all(v == total for v in sums.values()):
            time.sleep(15)          # dernier micro-batch (vide) : emission des fenetres fermees
            return
        time.sleep(10)
    raise TimeoutError(what)


def run_pipeline(app: str):
    spark = get_spark(app, delta=True, spark__sql__shuffle__partitions="3")
    spark.sparkContext.setLogLevel("ERROR")
    return spark, md.start_all(spark, which=("bronze", "silver", "gold_10min"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase1", type=int, default=10000)
    ap.add_argument("--phase2", type=int, default=6000)
    ap.add_argument("--rate", type=float, default=50)
    args = ap.parse_args()
    total = args.phase1 + args.phase2
    silver_ckpt = f"{md.CKPT}/p5_silver"

    print(f"=== PHASE 1 : {args.phase1} evenements, pipeline demarre ===", flush=True)
    produce(0, args.phase1, 2000)
    spark, queries = run_pipeline("pedali-restart-1")
    wait_caught_up(queries, args.phase1, "phase 1")
    qid_before = {q.name: str(q.id) for q in queries}
    for q in queries:
        q.stop()
    before = counts(spark)
    off_before = checkpoint_offsets(spark, silver_ckpt)
    print(f"[avant arret] comptages {before}\n[avant arret] checkpoint silver batch {off_before['batch']} offsets {off_before['offsets']}", flush=True)
    spark.stop()
    print("=== ARRET du pipeline (requetes et SparkSession arretees) ===", flush=True)

    print(f"=== PHASE 2 : le producteur continue pendant l'arret ({args.phase2} evt a {args.rate:.0f}/s) ===", flush=True)
    produce(args.phase1, args.phase2, args.rate)

    print("=== PHASE 3 : redemarrage avec les memes checkpoints ===", flush=True)
    spark, queries = run_pipeline("pedali-restart-2")
    qid_after = {q.name: str(q.id) for q in queries}
    wait_caught_up(queries, total, "phase 3")
    for q in queries:
        q.stop()
    after = counts(spark)
    off_after = checkpoint_offsets(spark, silver_ckpt)
    kafka_end = kafka_end_offsets(spark)
    spark.stop()

    print(f"\n[apres reprise] comptages {after}")
    print(f"[apres reprise] checkpoint silver batch {off_after['batch']} offsets {off_after['offsets']}")
    print(f"[kafka] fin du journal par partition {kafka_end} (total {sum(kafka_end.values())})")
    print(f"[queryId] avant {qid_before}\n[queryId] apres {qid_after}")

    spark = get_spark("pedali-restart-check", delta=True, spark__sql__shuffle__partitions="3")
    spark.sparkContext.setLogLevel("ERROR")
    kpi = spark.read.format("delta").load(md.GOLD_10MIN)
    print(f"[gold] kpi_city_10min : {kpi.count()} fenetres ; DESCRIBE HISTORY :")
    spark.sql(f"DESCRIBE HISTORY delta.`{md.GOLD_10MIN}`").select("version", "timestamp", "operation").show(10, truncate=False)
    spark.stop()

    assert after["bronze"] == total == sum(kafka_end.values()), "Bronze != nombre d'evenements Kafka"
    assert after["bronze"] == after["bronze_distincts"], "doublon d'offset en Bronze"
    assert after["silver"] == after["silver_distincts"], "doublon (trip_id, status) en Silver"
    assert after["silver"] >= before["silver"], "Silver a perdu des lignes"
    assert off_after["offsets"] == kafka_end, "les offsets du checkpoint ne sont pas a la fin du journal Kafka"
    assert off_after["batch"] > off_before["batch"], "le checkpoint n'a pas avance"
    assert qid_before == qid_after, "queryId different : le checkpoint n'a pas ete reutilise"
    print(f"\nRESULTAT : OK - reprise aux offsets du checkpoint, Bronze {after['bronze']} = Kafka {total}, "
          f"Silver {after['silver']} lignes sans doublon (ecart avec les evenements : doublons et retards > watermark ecartes)")


if __name__ == "__main__":
    main()
