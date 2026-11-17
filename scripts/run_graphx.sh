#!/usr/bin/env bash
# Lance graphx/GraphAnalysis.scala dans spark-shell (Scala 2.12) sur le cluster, depuis le conteneur Jupyter.
# Prerequis : python graphframes/graph_analysis.py a deja ecrit s3a://lake/gold/graph_export/ .
# Usage : bash scripts/run_graphx.sh
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MSYS_NO_PATHCONV=1 docker cp graphx/GraphAnalysis.scala pedali-jupyter:/tmp/GraphAnalysis.scala   # graphx/ n'est pas monte dans Jupyter
MSYS_NO_PATHCONV=1 docker exec pedali-jupyter bash -c '
cat /tmp/GraphAnalysis.scala > /tmp/run_graphx.scala
echo "GraphAnalysis.main(Array(\"s3a://lake\")); sys.exit(0)" >> /tmp/run_graphx.scala
/usr/local/spark/bin/spark-shell --master "$SPARK_MASTER" \
  --conf spark.driver.host=pedali-jupyter --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g --conf spark.cores.max=4 --conf spark.sql.shuffle.partitions=8 \
  --conf spark.hadoop.fs.s3a.endpoint="$S3_ENDPOINT" --conf spark.hadoop.fs.s3a.access.key="$S3_ACCESS_KEY" \
  --conf spark.hadoop.fs.s3a.secret.key="$S3_SECRET_KEY" --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  -i /tmp/run_graphx.scala < /dev/null
'
