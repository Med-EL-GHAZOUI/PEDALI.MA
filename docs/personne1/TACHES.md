# Personne 1 - Data Engineering

Responsabilite : infrastructure, Kafka, Spark, streaming et stockage. Rapports : `docs/rapport_jalon1.pdf`, `docs/rapport_jalon2.pdf`.

## Jalon 1 (Parties 1 a 3)
- [x] Depot, docker-compose (Kafka KRaft, topic `trips` 3 partitions, Spark Master + 2 Workers, MinIO + bucket `lake`, Jupyter) ; `tests/check_storage.sh` 7/7
- [x] SparkSession (version, master, appName, defaultParallelism) : `notebooks/01_spark_setup.ipynb`
- [x] RDD : CA par ville, duree moyenne par ville et type de vehicule, top 5 : `pipeline/rdd_analysis.py`, `notebooks/02_rdd.ipynb`
- [x] DataFrames, SQL, UDF `trip_band`, Parquet `silver/trips_history` : `pipeline/df_analysis.py`, `notebooks/03_dataframes_sql.ipynb`

## Jalon 2 (Parties 4 a 6)
- [x] Partie 4 : lecture Kafka, `from_json`, console `append`, JSON bruts `bronze/trips_raw`, comptages `complete`, watermark : `pipeline/part4_streaming.py`
- [x] Partie 5 : dedoublonnage + watermark, jointure stream-static, Silver Parquet (city, date), Gold Delta : `pipeline/medallion.py`
- [x] Checkpoints et test d'arret / reprise (2 min de production pendant l'arret) : `tests/check_pipeline_restart.py`, `reports/pipeline_restart_output.txt`
- [x] Preuves : `reports/pipeline_minio_layers.txt`, `pipeline_gold_evidence.txt` (DESCRIBE HISTORY), `kafka_consumer_groups.txt`
- [x] GraphFrames sur la couche Silver : `graphframes/graph_analysis.py` (lit `silver/trips_clean`)
- [ ] A rejouer avec le `producer.py` et le `stations.json` officiels de l'enseignant (ici reproduits d'apres le sujet)
- [ ] Soutenance de 10 minutes : demonstration du streaming et des graphes
