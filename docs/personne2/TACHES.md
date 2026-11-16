# Personne 2 - Data Analytics

Responsabilite : preparation et validation des donnees.
Branche Git : `personne2`

## Partie 1 - Environnement
- [x] MinIO + bucket `lake`
- [x] Jupyter (service configure)
- [x] Tests (`tests/check_storage.sh`) et documentation (`docs/partie1_environnement.md`)
- [x] Capture console MinIO (`screenshots/p2_minio_bucket_lake.png`) + sortie de check_storage.sh (`reports/minio_storage_check.txt`, 7/7 OK) ; capture Spark Master avec 2 Workers (`screenshots/p2_spark_master_2_workers.png`)

## Donnees (Parties 2 et 3)
- [x] Etude de trips_history.jsonl et stations.json, schema documente (`docs/schema_donnees.md`)
- [x] Jeu de donnees (20 100 trajets, 96 stations) + `generate_sample_data.py`
- [x] Controle qualite `validate_data.py`
- [x] Schema Spark explicite (`data/pedali_schema.py`)

## Partie 2 - RDD
- [x] Validation independante des resultats RDD / DataFrame (`notebooks/02b_p2_validation_rdd_df.ipynb`, execute en local ; a rejouer sur le cluster)
- [ ] Captures DAG et stages (Spark UI)

## Partie 3 - SQL / DataFrames
- [x] Referentiel stations, jointures (`notebooks/03b_p2_jointures_explain.ipynb`)
- [x] explain(True) / plan Catalyst, comparaison RDD vs DataFrame (meme notebook ; capture du plan a faire sur le cluster)

