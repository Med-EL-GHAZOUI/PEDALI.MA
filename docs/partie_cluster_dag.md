# Cluster : DAG, stages et premiers constats (Personne 2)

Job : `tests/cluster_dag_demo.py`, exécuté depuis Jupyter sur `spark://spark-master:7077` (2 Workers, 4 cœurs),
lecture directe de MinIO (`s3a://lake/raw/…`). Sortie : `reports/cluster_dag_demo_output.txt`.

Captures (`screenshots/`) :

| Fichier | Contenu |
|---|---|
| `p2_spark_master_2_workers.png` | Spark Master : 2 Workers ALIVE, 4 cœurs, 4 Go |
| `p2_spark_ui_jobs.png` | 5 jobs terminés + 1 job en échec (le RDD, voir plus bas) |
| `p2_spark_ui_dag_stages.png` | DAG du job 4 : stage 4 (réutilisé) puis stage 5 |
| `p2_spark_ui_sql_list.png`, `p2_spark_ui_sql_plan.png` | requête SQL/DataFrame et son plan d'exécution |

## Lecture du DAG (requête : filtre, jointure stations, agrégation, tri)

- **Stage 4 (sauté)** : `Scan json` (trajets) -> `WholeStageCodegen` (filtre `status = 'completed' AND user_id IS NOT NULL`,
  jointure, agrégation partielle) -> `Exchange` (**shuffle** : 24 enregistrements, 8 partitions).
  Il est affiché « skipped » dans le job 4, très probablement parce que ses sorties de shuffle ont déjà été produites par le job 3 (réutilisation).
- **Stage 5** : `AQEShuffleRead` -> `HashAggregate` final -> `TakeOrderedAndProject` (tri + limite pour `show(5)`).
- **Jointure** : `BroadcastHashJoin`. Le référentiel `stations` (12 lignes) est diffusé via `BroadcastExchange`
  (4 Mo) ; le gros côté (17 945 lignes lues, 17 857 après jointure) n'est **pas** shufflé. Seule l'agrégation par
  (ville, station) provoque un shuffle.
- **AQE (Adaptive Query Execution)** : `AQEShuffleRead` a fusionné les 8 partitions de shuffle en **1** partition
  (1 867 octets), ce qui évite 7 tâches inutiles sur un si petit résultat.
- Les filtres sont poussés dans la lecture : le scan des trajets ne sort que 17 945 lignes sur 20 100.

## Constats importants pour l'équipe

1. **Les RDD PySpark échouent sur ce cluster** (job 5, en échec) :
   `PYTHON_VERSION_MISMATCH : Python in worker has different version (3, 8) than that in driver 3.11`.
   Les Workers (`apache/spark:3.5.3`) embarquent Python 3.8 ; l'image Jupyter utilise Python 3.11.
   Les DataFrames sans UDF Python fonctionnent (pas de Python côté executors), mais **la Partie 2 (RDD) ne peut pas
   tourner sur le cluster telle quelle**. Pistes (Personne 1, `docker-compose.yml`) : utiliser pour les Workers une image
   alignée sur celle de Jupyter (même version de Python), ou fixer `PYSPARK_PYTHON` sur un Python identique des deux côtés.
2. **Les Workers ne voient pas `data/`** : il faut passer par MinIO. Les données ont été chargées dans
   `s3a://lake/raw/` (`trips_history.jsonl`, `stations.json`).
3. **S3A exige des jars absents de l'image Jupyter** : `hadoop-aws:3.3.4` et `aws-java-sdk-bundle:1.12.262`
   (≈ 275 Mo), téléchargés au premier lancement via `spark.jars.packages` (pas de cache : ~5 min la première fois).
   À préinstaller dans l'image ou un volume pour des démarrages rapides.
4. Pour que les Workers rappellent le driver, il faut `spark.driver.host=pedali-jupyter` (nom du conteneur sur
   `pedali-net`) et `spark.driver.bindAddress=0.0.0.0`.

## Limites

Les chiffres du job lisent les données brutes (sans dédoublonnage) : Casa Port = 2 849 trajets terminés ici contre
2 815 après nettoyage (`validate_data.py`). Un seul job de démonstration a été capturé ; le DAG détaillé de chaque
calcul RDD de la Partie 2 reste à faire une fois le décalage de version de Python corrigé.
