# PEDALI.MA

Plateforme Big Data pour un service de mobilite partagee (projet en binome).

## Organisation du depot

| Branche | Contenu |
|---|---|
| `main` | **Base commune seulement** : structure des dossiers, jeux de donnees (`data/stations.json`, `data/trips_history.jsonl`), schema des donnees (`docs/schema_donnees.md`), suivi des taches, README |
| `personne1` | Travail de la Personne 1 (Data Engineering) |
| `personne2` | Travail de la Personne 2 (Data Analytics) |

Le code de chaque personne n'est **pas** dans `main` : il se trouve sur sa branche.

## Repartition

| | Personne 1 | Personne 2 |
|---|---|---|
| Role | Data Engineering | Data Analytics |
| Perimetre | Infrastructure, Kafka, Spark, stockage | Donnees, validation, KPI, GraphFrames, GraphX, visualisation |
| Branche | `personne1` | `personne2` |
| Suivi des taches | [docs/personne1/TACHES.md](docs/personne1/TACHES.md) | [docs/personne2/TACHES.md](docs/personne2/TACHES.md) |

## Workflow Git

1. Chacun travaille sur sa branche (`personne1` ou `personne2`).
2. Commits prefixes : `Partie N (P1): ...` ou `Partie N (P2): ...`.
3. `main` ne recoit que ce qui est commun aux deux personnes (donnees, schema, documentation partagee).
4. Schemas, noms de colonnes et chemins MinIO sont definis ensemble (voir `docs/schema_donnees.md`).
