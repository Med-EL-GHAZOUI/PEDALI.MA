"""SparkSession commune (Personne 1) : cluster Docker, S3A -> MinIO, Delta Lake.

Les jars (hadoop-aws, Delta, Kafka, GraphFrames) sont pre-installes dans l'image `pedali-spark`
(docker/spark/Dockerfile) : aucun `spark.jars.packages`, demarrage immediat.
Hors Docker (SPARK_MASTER absent) : session locale, sans S3A.
"""
import os

from pyspark.sql import SparkSession

LAKE = "s3a://lake"
BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"
GOLD = f"{LAKE}/gold"
CHECKPOINTS = f"{LAKE}/checkpoints"
RAW = f"{LAKE}/raw"
KAFKA_SERVERS = os.environ.get("KAFKA_SERVERS", "kafka:9092")


def get_spark(app_name: str, delta: bool = False, **extra_conf) -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[2]")
    builder = SparkSession.builder.appName(app_name).master(master)
    if master.startswith("spark://"):
        builder = (builder
                   .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "pedali-jupyter"))
                   .config("spark.driver.bindAddress", "0.0.0.0")
                   .config("spark.executor.memory", "1g")
                   .config("spark.cores.max", "4")
                   .config("spark.scheduler.minRegisteredResourcesRatio", "1.0")
                   .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s"))
    if "S3_ENDPOINT" in os.environ:
        builder = (builder
                   .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
                   .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
                   .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                   .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    if delta:
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    builder = builder.config("spark.sql.shuffle.partitions", "8")
    for key, value in extra_conf.items():
        builder = builder.config(key.replace("__", "."), value)
    return builder.getOrCreate()
