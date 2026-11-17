# Partie 1 - Environnement (Personne 2 : MinIO, Jupyter, bucket lake)

## Services

| Service | URL | Identifiants (dev) |
|---|---|---|
| MinIO API (S3) | http://localhost:9000 | minioadmin / minioadmin123 |
| Console MinIO | http://localhost:9001 | idem |
| Jupyter | http://localhost:8888/?token=pedali | token `pedali` |
| Spark UI (driver) | http://localhost:4040 | - |

Bucket cree automatiquement : `lake` (couches `bronze/`, `silver/`, `gold/` creees par le pipeline).

## Lancement

```bash
cd docker
cp .env.example .env
# Avec le compose de la Personne 1 (Kafka + Spark) :
docker compose -f docker-compose.yml -f docker-compose.storage.yml up -d
# Seul (sans Spark, le test spark-master echouera) :
docker compose -f docker-compose.storage.yml up -d
bash ../tests/check_storage.sh
```

## Conventions a valider avec la Personne 1

- Reseau Docker commun : `pedali-net` (a declarer aussi dans son compose).
- Nom du service Spark Master : `spark-master`, port 7077.
- Nom du service Kafka : `kafka:9092`.
- Chemins : `s3a://lake/bronze/`, `s3a://lake/silver/`, `s3a://lake/gold/`.
- Acces S3A depuis Spark : endpoint `http://minio:9000`, `path.style.access=true`,
  jars `hadoop-aws` + `aws-java-sdk-bundle` compatibles avec Spark 3.5.3 (Hadoop 3.3.4).

## Versions

| Composant | Version |
|---|---|
| Spark (Jupyter) | 3.5.3 (a aligner sur le cluster) |
| MinIO | RELEASE.2024-10-13 |
| Delta Lake / GraphFrames / Kafka | a documenter (Personnes 1 et 2) |

## Preuves a capturer (dossier screenshots/)

- Console MinIO avec le bucket `lake`.
- Sortie de `tests/check_storage.sh`.
- Notebook Jupyter connecte au cluster (avec la Personne 1).
