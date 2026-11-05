# Schema des donnees et regles de qualite (Personne 2)

> Schema **suppose** : le sujet original n'est pas dans le depot. Les donnees actuelles sont
> synthetiques (`python data/generate_sample_data.py`, graine 42). A ajuster des que les vrais
> `trips_history.jsonl` et `stations.json` sont disponibles.

## stations.json (12 stations, 3 villes)

| Champ | Type Spark | Description |
|---|---|---|
| station_id | string | Cle (S01..S12) |
| name | string | Nom |
| city | string | Casablanca, Marrakech, Rabat |
| lat, lon | double | Coordonnees |
| capacity | int | Nombre de bornes |

## trips_history.jsonl (1 JSON par ligne)

| Champ | Type Spark | Nullable | Description |
|---|---|---|---|
| trip_id | string | non | Identifiant unique |
| user_id | string | non (qualite) | Utilisateur |
| bike_id | string | non | Velo |
| start_station_id | string | non | Station de depart |
| end_station_id | string | oui | Arrivee (null si non termine) |
| city | string | non | Ville du depart |
| start_time | timestamp | non | ISO 8601 |
| end_time | timestamp | oui | Null si non termine |
| duration_min | int | oui | Duree en minutes |
| price_mad | double | non | Prix en MAD (0 si non termine) |
| status | string | non | completed, cancelled, breakdown |

## Regles de qualite

1. `trip_id` unique : dedoublonner.
2. `user_id` non null.
3. `start_station_id` present dans le referentiel stations.
4. `duration_min` >= 0.
5. Un trajet `completed` a `end_time`, `end_station_id` et `duration_min`.
6. KPI calcules sur les trajets `completed` apres nettoyage ; taux de panne et d'annulation
   calcules sur tous les trajets valides.

## Anomalies injectees (donnees synthetiques)

~100 doublons, ~100 `user_id` nuls, ~100 stations inconnues (`S99`), ~91 durees negatives.
`python data/validate_data.py` les detecte et affiche les valeurs de reference
(revenus par ville, duree moyenne, top 5 stations) pour valider les resultats RDD/DataFrames.

## Scenarios (Partie 6)

- Station qui se vide : S01 (depart massif vers S02-S04).
- Boucle touristique : S05 -> S06 -> S07 -> S05.
- Flux pendulaires : S10 -> S11 le matin, S11 -> S10 le soir.
