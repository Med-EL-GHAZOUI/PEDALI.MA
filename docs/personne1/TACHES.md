# Personne 1 - Data Engineering

Responsabilite : infrastructure, Kafka, Spark et stockage.
Branche Git : `personne1`

## Partie 1 - Environnement
- [x] Depot Git et organisation des dossiers
- [x] docker-compose.yml : Kafka KRaft, topic `trips` (3 partitions)
- [x] Spark Master + 2 Workers
- [x] Verifier le lancement (`docker compose up`) : cluster OK, 2 Workers ALIVE (capture : `screenshots/p2_spark_master_2_workers.png`)
- [x] Notebook `01_spark_setup.ipynb` : SparkSession (version, master, appName, defaultParallelism)

## Partie 2 - RDD
- [ ] Chargement JSONL et calculs RDD : revenus par ville, duree moyenne, top 5 stations

## Partie 3 - SQL / DataFrames
- [ ] DataFrames, SQL, UDF, agregations
- [ ] Export Parquet

