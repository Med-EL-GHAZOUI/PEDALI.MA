# Partie 4 - Structured Streaming : watermark, retards, doublons, modes de sortie (Personne 2)

Code : `streaming/stream_kpi.py` (cœur `kpi_stream`, indépendant de la source).
Test : `tests/check_streaming.py` ; sortie de référence : `reports/streaming_test_output.txt`.

## Pipeline

```
source (Kafka topic `trips` | socket | fichiers JSONL)
  -> from_json(TRIPS_SCHEMA)                      # messages illisibles ecartes
  -> withWatermark("start_time", "10 minutes")
  -> qualite : user_id non nul, duree >= 0, station de depart connue
  -> dropDuplicatesWithinWatermark(["trip_id"])   # reemissions at-least-once
  -> fenetres fixes de 15 min sur start_time -> memes KPI que le batch (data/pedali_kpi.py)
```

La lecture Kafka est `read_kafka` ; sa version « cluster » (offsets, checkpoint sur MinIO, sink Delta) est du
ressort de la Personne 1. Le cœur de calcul et ses tests sont ici.

## Règle de retard observée

Avec un watermark de 10 min, après un lot dont le plus grand `start_time` est M, un événement est
**accepté si son `start_time` >= M - 10 min**, sinon écarté. Observé dans le test : T5x (10:08, watermark
10:10) est écarté **alors que sa fenêtre 10:00-10:15 est encore ouverte** ; T5 (10:11) arrivé 9 min après
le maximum est accepté et fait passer la fenêtre de 3 à 4 trajets.
Cause probable : l'opérateur de dédoublonnage applique lui-même le filtre watermark sur `start_time`
(plus strict que le seul critère « fin de fenêtre <= watermark » d'une agrégation seule). Pour tolérer
davantage de retard, il faut augmenter `WATERMARK` (au prix d'un état plus gros et d'une émission `append` plus tardive).

## Modes de sortie (résultats du test)

Scénario : 4 lots ; lot 1 `T1..T4` ; lot 2 retard toléré + retard rejeté + doublon + `T6` ; lot 3 retards rejetés,
3 lignes invalides, `T9` ; lot 4 fait avancer le watermark à 11:10.

| Mode | Comportement observé | Usage |
|---|---|---|
| `update` | Émet les fenêtres modifiées à chaque lot ; la fenêtre 10:00-10:15 est émise 2 fois (3 puis 4 trajets, après le retard toléré) | tableau de bord temps réel |
| `append` | Émet chaque fenêtre **une seule fois**, avec son résultat final, quand le watermark la ferme : 10:00 et 10:15 au micro-batch n°4, 10:30 et 10:45 au micro-batch n°8 (voir `reports/streaming_test_output.txt`) ; la fenêtre 11:15 (encore ouverte) n'est jamais émise | écriture finale (fichiers, Delta Bronze/Silver/Gold) |
| `complete` | Réémet tout le tableau à chaque micro-batch (jusqu'à 5 fenêtres) ; l'état ne se purge pas | petit volume, démo |

En `append`, le résultat n'apparaît qu'après un micro-batch qui suit le franchissement du watermark : sans
nouvelles données, Spark exécute un lot vide pour émettre les fenêtres fermées (`noDataMicroBatches`).

## Cas testés (`tests/check_streaming.py`)

| Cas | Événement | Résultat |
|---|---|---|
| Retard toléré | T5 (10:11, watermark 10:10) | accepté, la fenêtre est mise à jour |
| Retard rejeté, fenêtre encore ouverte | T5x (10:08) | écarté |
| Trop en retard | T7 (10:03), T8 (10:29) avec watermark 10:31 | écartés |
| Doublon | T1 rejoué dans un lot suivant | écarté (compté une fois) |
| Données invalides | `user_id` nul, durée négative, station `S99` | écartées |

Le test compare, pour les 3 modes, la sortie Spark à une simulation Python indépendante de ces règles.

## Exécuter

Le test a besoin de Linux : sous Windows, un flux avec état exige un checkpoint, donc `winutils` (Hadoop).
Il a été exécuté dans le conteneur Spark du projet :

```bash
docker run --rm -v "$PWD:/work:ro" \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip -e PYSPARK_PYTHON=python3 \
  apache/spark:3.5.3-python3 python3 /work/tests/check_streaming.py
```

Cluster (Kafka) : `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/stream_kpi.py --source kafka --servers kafka:9092 --mode update`
(**non exécuté** : nécessite le producteur Kafka de la Personne 1 et la pile Docker complète).

## Limites

- Testé avec une source socket ; la lecture Kafka (`read_kafka`) partage le même parsing mais n'a pas été exécutée.
- Les partitions Kafka ne garantissent pas l'ordre global : le watermark suit le plus grand `start_time` vu, toutes partitions confondues.
- Le test déclenche chaque lot manuellement ; le comportement sous débit continu (triggers temporels) n'est pas couvert.
