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

## Jalon 1 (Parties 1 a 3) - branche `jalon1`

Donnees au format du sujet (voir [schema](docs/schema_donnees.md)) ; rapport : [docs/rapport_jalon1.pdf](docs/rapport_jalon1.pdf).

```bash
python data/generate_stations.py && python producer/producer.py --mode file --n 50000
bash scripts/load_history.sh                       # historique -> s3a://lake/bronze/history/
# notebooks a executer dans Jupyter (http://localhost:8888) : 01, 02, 02b, 03, 03b
```

## Jalon 2 (Parties 4 a 6) - branche `jalon2`

```bash
# Partie 4 : streaming Kafka (console, JSON Bronze, comptages, watermark) - le producteur envoie en parallele
python producer/producer.py --mode kafka --bootstrap kafka:9092 --n 50000 --limit 9000 --rate 150     # dans Jupyter
python pipeline/part4_streaming.py --seconds 100
python tests/check_watermark.py                    # evenement en retard de 15 min
# Partie 5 : pipeline Bronze / Silver / Gold (Delta) et test d'arret / reprise
python pipeline/medallion.py --seconds 300
python tests/check_pipeline_restart.py             # ~10 min : arret, 2 min de production, reprise
# Partie 6 : graphes
python graphframes/graph_analysis.py               # GraphFrames + exports (s3a://lake/gold/graph_export, graph_analysis)
bash scripts/run_graphx.sh                         # GraphX (Scala) dans spark-shell
python graphframes/plot_network.py Marrakech Casablanca
```

Rapport : [docs/rapport_jalon2.pdf](docs/rapport_jalon2.pdf).

## Preuves (dossier `reports/`)

| Livrable | Fichier |
|---|---|
| Jalon 1 : explain(True), validation, DAG des RDD | `jalon1_*.txt`, `rdd_dag_stages.txt` |
| Partie 4 : sink console, JSON Bronze, watermark | `streaming_part4_console.txt`, `streaming_part4_bronze_minio.txt`, `streaming_watermark_demo.txt` |
| Partie 5 : couches Bronze / Silver / Gold dans MinIO | `pipeline_minio_layers.txt` |
| Partie 5 : DESCRIBE HISTORY du Gold, extraits Silver et Gold | `pipeline_gold_evidence.txt` |
| Partie 5 : `kafka-consumer-groups.sh --describe` | `kafka_consumer_groups.txt` |
| Partie 5 : arret / reprise sans perte ni doublon | `pipeline_restart_output.txt`, `pipeline_full_run_progress.txt` |
| Partie 6 : GraphFrames, GraphX, comparaison, exports | `graphframes_output.txt`, `graphx_output.txt`, `graph_comparison.txt`, `graph_analysis_minio.txt` |
| Partie 6 : cartes du reseau | `screenshots/jalon2_reseau_marrakech.png`, `screenshots/jalon2_reseau_casablanca.png` |
