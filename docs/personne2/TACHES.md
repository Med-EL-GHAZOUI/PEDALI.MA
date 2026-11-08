# Personne 2 - Data Analytics

Responsabilite : preparation/validation des donnees, GraphFrames, GraphX, visualisation.
Branche Git : `personne2`

## Partie 1 - Environnement
- [x] MinIO + bucket `lake`
- [x] Jupyter (service configure)
- [x] Tests (`tests/check_storage.sh`) et documentation (`docs/partie1_environnement.md`)
- [ ] Capture console MinIO + sortie de check_storage.sh

## Donnees (Parties 2 et 3)
- [x] Etude de trips_history.jsonl et stations.json, schema documente (`docs/schema_donnees.md`)
- [x] Jeu de donnees (20 100 trajets, 96 stations) + `generate_sample_data.py`
- [x] Controle qualite `validate_data.py`
- [ ] Schema Spark explicite

## Partie 2 - RDD
- [ ] Validation independante des resultats RDD
- [ ] Captures DAG et stages (Spark UI)

## Partie 3 - SQL / DataFrames
- [ ] Referentiel stations, jointures
- [ ] explain(True) / plan Catalyst, comparaison RDD vs DataFrame

## Partie 4 - Streaming
- [ ] Watermark, evenements en retard, doublons, modes de sortie, tests

## Partie 5 - Pipeline
- [ ] Fenetres temporelles et KPI (trajets, CA, duree, taux de panne, taux d'annulation)
- [ ] Delta Lake : DESCRIBE HISTORY, tests de reprise

## Partie 6 - Graphes
- [ ] Sommets/aretes depuis Silver
- [ ] Motifs GraphFrames, degres, soldes nets, stations source/puits
- [ ] GraphX (Scala) : degrees, PageRank, connectedComponents, ShortestPaths
- [ ] Comparaison, seuils de reequilibrage, visualisation, scenarios injectes
