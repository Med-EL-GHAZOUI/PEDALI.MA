# Image commune Spark / Jupyter (Partie 1 - Personne 1).
# Meme image pour Master, Workers et Jupyter => meme Python (3.11) partout, ce qui supprime
# l'erreur PYTHON_VERSION_MISMATCH des RDD, et jars S3A / Delta / Kafka / GraphFrames pre-installes
# (plus de telechargement Ivy a chaque demarrage).
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3
USER root
ARG MVN=https://repo1.maven.org/maven2
RUN set -e; cd /usr/local/spark/jars; \
    for p in \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar \
      io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      io/graphframes/graphframes-spark3_2.12/0.10.0/graphframes-spark3_2.12-0.10.0.jar ; do \
        curl -fsSL -O "$MVN/$p"; \
    done
RUN pip install --no-cache-dir delta-spark==3.2.0 graphframes-py==0.10.0 kafka-python==2.0.2 matplotlib
ENV PYSPARK_PYTHON=/opt/conda/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python
USER jovyan
