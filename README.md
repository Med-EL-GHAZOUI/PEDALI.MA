# PEDALI.MA

Plateforme Big Data pour un service de mobilite partagee (projet en binome).

## Repartition

| | Personne 1 | Personne 2 |
|---|---|---|
| Role | Data Engineering | Data Analytics |
| Perimetre | Infrastructure, Kafka, Spark, streaming, stockage | Donnees, validation, GraphFrames, GraphX, visualisation |
| Branche | `personne1` | `personne2` |
| Suivi des taches | [docs/personne1/TACHES.md](docs/personne1/TACHES.md) | [docs/personne2/TACHES.md](docs/personne2/TACHES.md) |

## Workflow Git

1. Chacun travaille sur sa branche (`personne1` ou `personne2`).
2. Commits prefixes : `Partie N (P1): ...` ou `Partie N (P2): ...`.
3. Fusion reguliere vers `main` apres revue de l'autre membre.
4. Schemas, noms de colonnes et chemins MinIO sont definis ensemble (voir `docs/`).

## Lancement

```bash
cd docker
cp .env.example .env     # si 9000/9001 sont pris : MINIO_API_PORT=9010, MINIO_CONSOLE_PORT=9011
docker compose -f docker-compose.yml -f docker-compose.storage.yml up -d --build
bash ../tests/check_storage.sh      # 7 controles (MinIO, bucket lake, Jupyter, Spark)
```

Interfaces : Spark Master http://localhost:8080 - Jupyter http://localhost:8888 - MinIO (console) selon `.env`.

## Versions compatibles

| Composant | Version |
|---|---|
| Spark / PySpark | 3.5.3 (Scala 2.12) |
| Python (Master, Workers, Jupyter) | 3.11 |
| Kafka | 3.8.0 (mode KRaft) |
| Delta Lake | 3.2 |
| GraphFrames | 0.10.0 |
| hadoop-aws (S3A) | 3.3.4 |
| MinIO | image Chainguard |

## Documentation

- [Partie 1 - Environnement](docs/partie1_environnement.md)
- [Schema des donnees](docs/schema_donnees.md)
- [Cluster : DAG, stages, RDD](docs/partie_cluster_dag.md)
- [Partie 4 - Streaming](docs/partie4_streaming.md)
- [Partie 5 - Delta Lake](docs/partie5_delta.md) et [pipeline (Personne 1)](docs/personne1/PIPELINE.md)
- [Partie 6 - Graphes](docs/partie6_graphes.md)
- [Rapport de projet (PDF, 5 pages)](docs/rapport_pedali.pdf)

## Preuves (dossier `reports/`)

| Livrable | Fichier |
|---|---|
| Couches Bronze / Silver / Gold dans MinIO | `minio_layers.txt` |
| DESCRIBE HISTORY de la table Gold | `delta_describe_history_gold.txt` |
| `kafka-consumer-groups.sh --describe` | `kafka_consumer_groups.txt` |
| Sink console et micro-batches | `streaming_console_microbatches.txt` |
| DAG / stages des RDD | `rdd_dag_stages.txt` |
| Test arret/redemarrage du pipeline | `pipeline_restart_output.txt` |
