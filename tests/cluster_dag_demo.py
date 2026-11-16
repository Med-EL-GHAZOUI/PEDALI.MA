"""Job de demonstration sur le cluster (Jupyter -> spark-master -> MinIO) pour les captures DAG / stages."""
import os
import sys
import time

sys.path.insert(0, "/home/jovyan/work/data")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("pedali-p2-dag")
         .master(os.environ["SPARK_MASTER"])
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
         .config("spark.jars.ivy", "/tmp/ivy")
         .config("spark.driver.host", "pedali-jupyter")
         .config("spark.driver.bindAddress", "0.0.0.0")
         .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
         .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
         .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.executor.memory", "1g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
sc = spark.sparkContext
print("VERSION", spark.version, "MASTER", sc.master, "APP", sc.appName, "PARALLELISM", sc.defaultParallelism, flush=True)

trips = spark.read.json("s3a://lake/raw/trips_history.jsonl")
stations = spark.read.option("multiLine", True).json("s3a://lake/raw/stations.json")

# --- Job DataFrame : jointure + agregation (plusieurs stages : scan, shuffle, tri)
res = (trips.filter("status = 'completed' AND user_id IS NOT NULL")
       .join(F.broadcast(stations.select(F.col("station_id").alias("start_station_id"), "name")), "start_station_id")
       .groupBy("city", "name").agg(F.count("*").alias("trajets"), F.round(F.sum("price_mad"), 1).alias("revenu"))
       .orderBy(F.desc("trajets")))
res.show(5, truncate=False)
res.explain(True)

# --- Job RDD : reduceByKey (shuffle) ; necessite le meme Python sur les workers
try:
    out = trips.rdd.map(lambda r: (r["city"], 1)).reduceByKey(lambda a, b: a + b).collect()
    print("RDD_OK", sorted(out), flush=True)
except Exception as e:  # noqa: BLE001
    print("RDD_FAIL", type(e).__name__, str(e)[:600], flush=True)

print("READY_FOR_SCREENSHOT", flush=True)
time.sleep(int(os.environ.get("HOLD", "900")))
spark.stop()
