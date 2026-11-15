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
- [x] Schema Spark explicite (`data/pedali_schema.py`)

## Partie 2 - RDD
- [x] Validation independante des resultats RDD / DataFrame (`notebooks/02b_p2_validation_rdd_df.ipynb`, execute en local ; a rejouer sur le cluster)
- [ ] Captures DAG et stages (Spark UI)

## Partie 3 - SQL / DataFrames
- [x] Referentiel stations, jointures (`notebooks/03b_p2_jointures_explain.ipynb`)
- [x] explain(True) / plan Catalyst, comparaison RDD vs DataFrame (meme notebook ; capture du plan a faire sur le cluster)

## Partie 4 - Streaming
- [ ] Watermark, evenements en retard, doublons, modes de sortie, tests

## Partie 5 - Pipeline
- [x] KPI definis et verifies (trajets, CA, duree, taux de panne, taux d'annulation) : `data/pedali_kpi.py` + `tests/check_kpi.py` (Spark == Python pur). Fenetres en streaming : a faire (Partie 4/5)
- [ ] Delta Lake : DESCRIBE HISTORY, tests de reprise

## Partie 6 - Graphes
- [x] Sommets/aretes (depuis les donnees nettoyees ; a rebrancher sur la vraie couche Silver)
- [x] Motifs GraphFrames, degres, soldes nets, stations source/puits (`graphframes/graph_analysis.py`)
- [x] GraphX (Scala) : degrees, PageRank, connectedComponents, ShortestPaths (`graphx/GraphAnalysis.scala`, compile et execute en local)
- [x] Comparaison, seuils de reequilibrage, visualisation, scenarios injectes (`docs/partie6_graphes.md`, `screenshots/reseau_stations.png`)
