# Personne 1 - Data Engineering

Responsabilite : infrastructure, Kafka, Spark, streaming et stockage.
Branche Git : `personne1`

## Partie 1 - Environnement
- [x] Depot Git et organisation des dossiers
- [x] docker-compose.yml : Kafka KRaft, topic `trips` (3 partitions)
- [x] Spark Master + 2 Workers
- [x] Verifier le lancement (`docker compose up`) : cluster OK, 2 Workers ALIVE (capture : `screenshots/p2_spark_master_2_workers.png`)
- [x] Notebook `01_spark_setup.ipynb` : SparkSession (version, master, appName, defaultParallelism)

## Partie 2 - RDD
- [x] Chargement JSONL et calculs RDD : revenus par ville, duree moyenne, top 5 stations (identiques a la reference P2, voir `notebooks/02_rdd.ipynb`)

## Partie 3 - SQL / DataFrames
- [x] DataFrames, SQL, UDF, agregations (`notebooks/03_dataframes_sql.ipynb`)
- [x] Export Parquet (`s3a://lake/export/parquet/`)

## Partie 4 - Streaming
- [x] `producer/producer.py` (cle = start_station_id, debit configurable)
- [x] Lecture Kafka (Structured Streaming) + parsing JSON + sinks Parquet/console (`pipeline/kafka_stream.py`, `notebooks/04_streaming_kafka.ipynb`)

## Partie 5 - Pipeline
- [ ] Ecritures Bronze et Silver, indicateurs Gold (Delta Lake)
- [ ] Checkpoints + test arret/redemarrage

## Partie 6 - Graphes
- [ ] GraphFrames (PySpark), avec la Personne 2
