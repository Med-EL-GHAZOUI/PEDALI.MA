"""Lance le pipeline Kafka -> Bronze -> Silver -> Gold jusqu'a ce que Silver atteigne --silver-rows (ou timeout).

Usage (Jupyter) : python pipeline/run_pipeline.py --silver-rows 19709
"""
import argparse
import sys
import time

sys.path.append("/home/jovyan/work/pipeline")
from pedali_spark import get_spark
import medallion as md

ap = argparse.ArgumentParser()
ap.add_argument("--silver-rows", type=int, required=True)
ap.add_argument("--timeout", type=int, default=600)
args = ap.parse_args()

spark = get_spark("pedali-pipeline", delta=True)
spark.sparkContext.setLogLevel("ERROR")
queries = md.start_all(spark)
end, n = time.time() + args.timeout, 0
while time.time() < end and n < args.silver_rows:
    time.sleep(5)
    n = spark.read.format("delta").load(md.SILVER).count()
    print("Silver =", n, flush=True)
time.sleep(15)  # dernier rafraichissement de l'or
for q in queries:
    q.stop()
spark.stop()
