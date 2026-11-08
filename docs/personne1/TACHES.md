# Personne 1 - Data Engineering

Responsabilite : infrastructure, Kafka, Spark, streaming et stockage.
Branche Git : `personne1`

## Partie 1 - Environnement
- [x] Depot Git et organisation des dossiers
- [x] docker-compose.yml : Kafka KRaft, topic `trips` (3 partitions)
- [x] Spark Master + 2 Workers
- [ ] Verifier le lancement (`docker compose up`) + capture Spark Master avec 2 Workers
- [ ] Notebook `01_spark_setup.ipynb` : SparkSession (version, master, appName, defaultParallelism)

## Partie 2 - RDD
- [ ] Chargement JSONL et calculs RDD : revenus par ville, duree moyenne, top 5 stations

## Partie 3 - SQL / DataFrames
- [ ] DataFrames, SQL, UDF, agregations
- [ ] Export Parquet

## Partie 4 - Streaming
- [ ] `producer/producer.py`
- [ ] Lecture Kafka (Structured Streaming) + parsing JSON + sinks

## Partie 5 - Pipeline
- [ ] Ecritures Bronze et Silver, indicateurs Gold (Delta Lake)
- [ ] Checkpoints + test arret/redemarrage

## Partie 6 - Graphes
- [ ] GraphFrames (PySpark), avec la Personne 2
