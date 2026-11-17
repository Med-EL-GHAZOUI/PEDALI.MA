# Personne 2 - Data Analytics

Responsabilite : preparation / validation des donnees, fenetres et KPI, GraphFrames, GraphX, visualisation.

## Jalon 1 (Parties 1 a 3)
- [x] Etude des donnees, schema explicite et regles de qualite : `docs/schema_donnees.md`, `data/pedali_schema.py`, `data/validate_data.py`
- [x] Validation independante Python pur == RDD == DataFrame : `notebooks/02b_p2_validation_rdd_df.ipynb`
- [x] DAG et stages (Spark UI) : `screenshots/jalon1_rdd_*.jpg`, `reports/rdd_dag_stages.txt`
- [x] Jointures stations, `explain(True)`, Broadcast vs SortMerge, RDD / DataFrame / SQL : `notebooks/03b_p2_jointures_explain.ipynb`

## Jalon 2 (Parties 4 a 6)
- [x] Watermark, evenements en retard (15 min ecarte, 5 min accepte) : `tests/check_watermark.py`, `reports/streaming_watermark_demo.txt`
- [x] Fenetres tumbling 10 min (Gold `kpi_city_10min`), sliding 10/5 min (taux de panne, d'annulation), session 5 min par utilisateur : `pipeline/medallion.py`
- [x] Sommets / aretes (par periode, sans aretes orphelines), motifs GraphFrames (allers-retours, boucles a 3 stations, corridors), degres, soldes nets par periode
- [x] GraphX (Scala) : degres, PageRank (non pondere et pondere), connectedComponents, ShortestPaths : `graphx/GraphAnalysis.scala`, `scripts/run_graphx.sh`
- [x] Comparaison GraphX / GraphFrames et validation des 4 scenarios injectes : `graphframes/compare_graphs.py`, `reports/graph_comparison.txt`
- [x] Seuils de reequilibrage et couples source -> cible, exports Parquet `gold/graph_analysis`, cartes du reseau : `graphframes/plot_network.py`
