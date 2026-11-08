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

## Documentation

- [Partie 1 - Environnement](docs/partie1_environnement.md)
- [Schema des donnees](docs/schema_donnees.md)
