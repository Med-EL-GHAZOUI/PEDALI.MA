# Schema des donnees et regles de qualite (format du sujet Pedali.ma)

> Le `producer.py` et le `stations.json` officiels ne sont pas dans le depot : `producer/producer.py` et
> `data/generate_stations.py` les reproduisent a partir de la description du sujet (meme schema, memes statuts,
> ~5 % d'evenements en retard, ~2 % de doublons, scenarios injectes). Les remplacer par les fichiers officiels
> des qu'ils sont disponibles : les noms de champs sont ceux du sujet.

## Generer les donnees

```bash
python data/generate_stations.py                      # data/stations.json : 400 stations, 4 villes
python producer/producer.py --mode file --n 50000     # data/trips_history.jsonl : 50 000 evenements
bash scripts/load_history.sh                          # copie dans s3a://lake/bronze/history/
python producer/producer.py --mode kafka --rate 20    # flux Kafka (topic trips), 20 evenements/s
```

## stations.json (400 stations : 100 par ville, Casablanca / Rabat / Marrakech / Tanger)

| Champ | Type Spark | Description |
|---|---|---|
| station_id | string | `S-CAS-042` (CAS, RAB, MAR, TAN) |
| name | string | Nom |
| city | string | Ville |
| district | string | Quartier |
| zone_type | string | gare, universite, plage, centre_affaires, residentiel, tourisme, commercial |
| capacity | int | Nombre de bornes |
| latitude, longitude | double | Coordonnees |

## trips_history.jsonl : un evenement JSON par changement d'etat

| Champ | Type Spark | Description |
|---|---|---|
| trip_id | string | `T-00078214` |
| user_id | string | `U-20418` |
| vehicle_id | string | `V-EB-0931` |
| vehicle_type | string | BIKE, EBIKE, SCOOTER |
| start_station | string | Station de depart |
| end_station | string | **ENDED seulement** (null sinon) |
| city | string | Ville |
| status | string | RESERVED, UNLOCKED, ENDED, CANCELLED, FAULT_REPORTED |
| duration_s | int | **ENDED seulement** |
| distance_km | double | **ENDED seulement** |
| battery_pct | int | null pour BIKE |
| price_mad | double | **ENDED seulement** |
| event_time | timestamp | ISO 8601 UTC (`2026-11-15T08:14:32Z`) |

Cycle de vie : `RESERVED -> UNLOCKED -> ENDED | FAULT_REPORTED`, ou `RESERVED -> CANCELLED`.

## Regles de qualite

1. Un evenement est identifie par `(trip_id, status)` : dedoublonner avec `dropDuplicates(["trip_id", "status"])`.
2. `start_station` doit exister dans le referentiel.
3. `end_station`, `duration_s`, `distance_km`, `price_mad` ne sont renseignes que pour ENDED.
4. Les KPI de trajets terminent se calculent sur ENDED apres dedoublonnage.

## Anomalies et scenarios injectes

- ~2 % de doublons (meme `trip_id`, meme `status`) ; ~5 % d'evenements en retard (jusqu'a 15 min, dates hors ordre dans le fichier).
- Flux pendulaires : residentiel -> centre d'affaires le matin, sens inverse le soir.
- Station qui se vide : `S-CAS-005` (beaucoup de departs, jamais d'arrivee).
- Boucle touristique A -> B -> C -> A entre 3 stations `tourisme` de Marrakech.
- Vehicule `V-SC-0666` (Tanger) qui accumule les signalements FAULT_REPORTED.

`python data/validate_data.py` calcule en Python pur la reference (CA par ville, duree moyenne par ville et type, top 5).
