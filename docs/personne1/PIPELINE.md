# Personne 1 - Infrastructure et chaîne de traitement

## Lancement

```bash
cd docker
cp .env.example .env            # si 9000/9001 sont pris : MINIO_API_PORT=9010, MINIO_CONSOLE_PORT=9011
docker compose -f docker-compose.yml -f docker-compose.storage.yml up -d --build
```

Image commune `pedali-spark` (`docker/spark/Dockerfile`) pour Master, Workers et Jupyter : même Python 3.11 partout
(corrige l'erreur `PYTHON_VERSION_MISMATCH` des RDD) et jars pré-installés (S3A, Delta 3.2, Kafka, GraphFrames 0.10).
Les Workers voient `data/` au même chemin que Jupyter (`/home/jovyan/work/data`).

## Contenu par partie

| Partie | Fichiers | Résultat |
|---|---|---|
| 1 | `docker/`, `notebooks/01_spark_setup.ipynb`, `pipeline/pedali_spark.py` | Spark 3.5.3, master `spark://spark-master:7077`, parallélisme 4 |
| 2 | `pipeline/rdd_analysis.py`, `notebooks/02_rdd.ipynb` | revenus/ville, durée moy. 17,56 min, top 5 : identiques à la référence P2 |
| 3 | `pipeline/df_analysis.py`, `notebooks/03_dataframes_sql.ipynb` | DataFrame == SQL, UDF de durée, Parquet dans `s3a://lake/export/parquet/` |
| 4 | `producer/producer.py`, `pipeline/kafka_stream.py`, `notebooks/04_streaming_kafka.ipynb` | topic `trips` (3 partitions, clé = station), sinks Parquet + KPI |
| 5 | `pipeline/medallion.py`, `tests/check_pipeline_restart.py` | Bronze/Silver/Gold Delta, checkpoints, test arrêt/redémarrage OK |
| 6 | `pipeline/graph_on_silver.py` | GraphFrames (code P2) lu depuis Silver : arêtes identiques à l'analyse sur fichiers |

## Pipeline médaillon (Partie 5)

```
Kafka trips -> BRONZE s3a://lake/bronze/trips   (brut + partition/offset)
            -> SILVER s3a://lake/silver/trips   (JSON typé, qualité, MERGE sur trip_id : rejouable sans doublon)
            -> GOLD   s3a://lake/gold/kpi_city_day, gold/top_stations
```

Un checkpoint par étape (`s3a://lake/checkpoints/{bronze,silver,gold}`). Test d'arrêt/redémarrage
(`reports/pipeline_restart_output.txt`) : 5 000 messages traités, arrêt de la session, 5 000 messages produits pendant
l'arrêt, redémarrage : Bronze = 10 000 (offsets tous distincts), Silver = 9 836 et Gold = 8 827 trajets / 232 642,5 MAD,
identiques à une référence Python indépendante ; les `queryId` sont conservés.

## Points d'attention

- Windows/Git Bash : préfixer `docker exec` de `MSYS_NO_PATHCONV=1` pour les chemins de conteneur.
- Les modules Python utilisés dans des RDD/UDF sont envoyés aux Workers avec `addPyFile` et n'importent rien d'inaccessible au niveau module.
- Le Gold est recalculé en entier à chaque micro-batch (petit volume). Pour un gros volume : agrégation incrémentale.
- Le topic `trips` contient actuellement les 20 100 lignes du fichier (doublons inclus).
