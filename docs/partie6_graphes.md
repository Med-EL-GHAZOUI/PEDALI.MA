# Partie 6 - Analyse du reseau de stations (Personne 2)

Donnees : `data/trips_history.jsonl` nettoye (equivalent de la couche Silver), 1er-30 octobre 2026,
17 673 trajets termines. **Sommets** = 12 stations ; **aretes** = trajets termines agreges par paire
(36 aretes, attributs : nombre de trajets, revenu).

> Les resultats ci-dessous viennent des donnees synthetiques du depot et ont ete obtenus en local
> (Spark 3.5.3, GraphFrames 0.10.0). Ils sont a rejouer sur les vraies donnees / la couche Silver.

## Executer

```bash
pip install pyspark==3.5.3 graphframes-py==0.10.0 pandas pyarrow matplotlib
python graphframes/graph_analysis.py        # GraphFrames + exports (reports/graph_exports/)
python graphframes/plot_network.py          # carte -> screenshots/reseau_stations.png
# GraphX : voir graphx/README.md
```

## Structure du reseau

- Le graphe est forme de **3 composantes connexes = les 3 villes** (S01-S04, S05-S08, S09-S12) ; aucun
  trajet entre villes. Chaque station a exactement 3 voisins entrants et 3 sortants (graphe complet par ville).
- Consequence : les degres et le PageRank **non pondere** (1,0 partout) ne distinguent rien. Il faut
  raisonner sur les **poids** (nombre de trajets) : degres ponderes, PageRank pondere, soldes nets.

## Motifs GraphFrames (seuil : 50 trajets par arete)

| Motif | Resultat |
|---|---|
| Corridors les plus frequentes | S11->S10 (1 502), S10->S11 (1 221), S01->S02/S03/S04 (~920-975), boucle S05->S06->S07->S05 (~770-840) |
| Allers-retours | S10<->S11 domine (2 723 trajets, 1 221 / 1 502) ; les paires S01<->Sx sont tres asymetriques (~950 / ~340) |
| Boucles a 3 stations | S05->S06->S07->S05 en tete (2 434 trajets, aretes ~800 chacune) |

## Soldes nets et stations a reequilibrer

Solde net = arrivees - departs (sur 30 jours). Negatif : la station se vide ; positif : elle sature.

| Station | Capacite | Solde / jour | Lecture |
|---|---|---|---|
| S01 Casa Port | 20 | **-59** | source : vide en < 1 jour sans reapprovisionnement |
| S11 Rabat Ville | 30 | -8 | source |
| S02 Maarif / S03 Ain Diab / S04 Hay Hassani | 25 / 30 / 15 | +20 / +20 / +19 | puits : recoivent les velos de S01 |
| S10 Hay Riad | 20 | +11 | puits |
| autres (Marrakech, S09, S12) | | |solde| < 2 | equilibrees |

Par periode, **S10 et S11** sont des stations pendulaires : S10 perd ~20 velos/jour le matin (99 % de sa
capacite) et en gagne ~23 le soir ; S11 fait l'inverse (+21 le matin, -21 le soir).

### Seuils de reequilibrage proposes

1. **Alerte** : |solde net journalier| >= 50 % de la capacite de la station sur une periode (matin 6-10 h,
   journee 10-16 h, soir 16-21 h, nuit 21-6 h). Declenche pour S01 (toutes periodes), S10 (matin, soir),
   S11 (matin, soir) et S04 (soir : 51 %).
2. **Urgence** : |solde net journalier| >= 80 % de la capacite -> S01 (matin 88 %, soir 97 %), S10 (matin 99 %, soir 114 %).
3. **Action** : S01 = source -> camion de reapprovisionnement depuis S02/S03/S04 (puits) en fin de journee ;
   S10/S11 = rotation a faire a la mi-journee (avant le pic du soir), dans le sens inverse du flux du matin.

## Validation des scenarios injectes

| Scenario (docs/schema_donnees.md) | Constate |
|---|---|
| Station qui se vide (S01) | OUI : solde -1 779, role SOURCE, S01->S02/S03/S04 en tete des corridors, 2 815 departs pour 1 036 arrivees |
| Boucle touristique S05->S06->S07->S05 | OUI : boucle a 3 stations la plus frequente (2 434), aretes ~770-840 |
| Flux pendulaires S10<->S11 | OUI : paire d'allers-retours n°1 ; S10 -596 / S11 +622 le matin, inverse le soir |

## Comparaison GraphX / GraphFrames

| | GraphFrames (PySpark) | GraphX (Scala) |
|---|---|---|
| Modele | DataFrames, motifs `find("(a)-[e]->(b)")` | RDD de sommets / aretes, API Pregel |
| Utilise ici pour | motifs (allers-retours, boucles), degres, soldes | degres, PageRank, composantes connexes, plus courts chemins |
| Degres | 3 / 3 pour chaque station | identique (3 / 3) |
| Composantes | - | 3 (une par ville) : coherent avec l'absence de trajets inter-villes |
| Centralite | degres ponderes : S01 (2 815 departs), S11, S10 | PageRank pondere : **S10 (1,32), S11 (1,24)**, puis Marrakech ; Rabat S09/S12 les plus bas (~0,72) |
| Points forts | lisibilite des motifs, integration SQL | algorithmes de graphe classiques (PageRank, CC, ShortestPaths) |
| Limite | pas d'API Pregel Python aussi riche | `pageRank` ignore les poids : PageRank pondere reimplemente (`weightedPageRank`) |

Les deux bibliotheques lisent le meme graphe (GraphX relit les exports de GraphFrames) et sont coherentes.
A noter : le PageRank pondere classe S10/S11 en tete (flux pendulaires), alors que S01, premier en departs,
n'a qu'un PageRank moyen (0,99, 9e sur 12) car il **emet** beaucoup mais **recoit** peu : le PageRank
mesure l'attractivite d'arrivee, pas le volume de departs.

## Exports (reports/graph_exports/)

Parquet (`sommets`, `aretes`, `corridors`, `allers_retours`, `boucles_3`, `degres`, `soldes_nets`,
`soldes_nets_periode`) et leurs equivalents CSV dans `csv/` (Gephi, Excel, lecture GraphX locale).
Carte du reseau : `screenshots/reseau_stations.png`.
