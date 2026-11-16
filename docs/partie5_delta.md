# Partie 5 - Couche Gold en Delta Lake et reprise sur checkpoint (Personne 2)

Test : `tests/check_delta_recovery.py` ; historique Delta : `reports/delta_describe_history.txt`.
Réutilise le calcul de la Partie 4 (`streaming/stream_kpi.py`) avec une table Delta comme sink.

## Ce qui est testé

1. Requête `kpi_stream` -> table Delta, mode **append**, `checkpointLocation` ; lots 1 et 2 traités.
2. **Arrêt** de la requête et de la SparkSession.
3. Dépôt des lots 3 et 4 **pendant l'arrêt** (retards, 3 lignes invalides, T6 rejoué = doublon).
4. **Reprise** : nouvelle session, même checkpoint.
5. Vérification : table Gold == simulation Python indépendante.

Résultat : 4 fenêtres (10:00, 10:15, 10:30, 10:45), **chacune une seule fois** ; la fenêtre 10:00 vaut 4 trajets /
3 terminés / 67,5 MAD (le retard toléré T5 est inclus, T5x et T7 rejetés). T6 n'est compté qu'une fois : l'état de
dédoublonnage et le watermark ont donc bien été **restaurés depuis le checkpoint**.

## DESCRIBE HISTORY (extrait)

| version | epochId | lignes écrites | fichiers ajoutés | moment |
|---|---|---|---|---|
| 0 | 0 | 0 | 0 | avant l'arrêt |
| 1 | 1 | 0 | 0 | avant l'arrêt |
| 2 | 2 | 0 | 0 | avant l'arrêt |
| 3 | 3 | **2** (fenêtres 10:00 et 10:15) | 1 | avant l'arrêt |
| 4 | 4 | 0 | 0 | **après la reprise** |
| 5 | 5 | **2** (fenêtres 10:30 et 10:45) | 2 | après la reprise |

Preuves de la reprise :
- les `epochId` continuent à 4 puis 5 au lieu de repartir de 0 ;
- le `queryId` est le même avant et après l'arrêt ;
- les deux fenêtres déjà publiées avant l'arrêt ne sont pas réécrites (pas de doublon en Gold).

Les versions « 0 lignes » sont les micro-batches qui ne ferment aucune fenêtre : en mode append, Delta enregistre
quand même une version par micro-batch.

## Exécuter

Linux requis (checkpoint Hadoop ; sous Windows, `winutils` est nécessaire). Dans le conteneur Spark du projet :

```bash
docker run --rm -v "$PWD:/work" -e PYTHONDONTWRITEBYTECODE=1 -e HOME=/tmp \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip -e PYSPARK_PYTHON=python3 \
  apache/spark:3.5.3-python3 python3 /work/tests/check_delta_recovery.py
```

Version : Delta Lake 3.2.0 (`io.delta:delta-spark_2.12:3.2.0`), compatible Spark 3.5.x.

## Limites

- La table Gold est écrite sur le disque du conteneur (`/tmp/pedali-gold`), pas sur `s3a://lake/gold/` : le chemin MinIO
  exige les jars `hadoop-aws` / `aws-java-sdk-bundle` et relève de la couche stockage de la Personne 1.
- La source du test est un dossier de fichiers (une reprise sur offsets Kafka n'a pas été testée).
- Le test arrête proprement la requête ; un arrêt brutal (kill du process en plein micro-batch) n'est pas couvert.
