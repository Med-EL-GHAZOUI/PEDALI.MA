"""Genere le referentiel stations.json : 400 stations, 4 villes, types de zone (format du sujet).

Champs : station_id (S-CAS-042), name, city, district (quartier), zone_type, capacity (bornes), latitude, longitude.
A remplacer par le fichier fourni par l'enseignant des qu'il est disponible (meme schema).

Usage : python data/generate_stations.py [graine]
"""
import json
import random
import sys
from pathlib import Path

OUT = Path(__file__).parent / "stations.json"
random.seed(int(sys.argv[1]) if len(sys.argv) > 1 else 42)

# ville : (prefixe, centre lat/lon, quartiers, poids des types de zone)
CITIES = {
    "Casablanca": ("CAS", (33.5731, -7.5898),
                   ["Maarif", "Ain Diab", "Hay Hassani", "Anfa", "Sidi Maarouf", "Bourgogne", "Roches Noires", "Oasis", "Ain Sebaa", "Casa Port"],
                   {"residentiel": 30, "centre_affaires": 20, "gare": 8, "universite": 10, "plage": 12, "tourisme": 5, "commercial": 15}),
    "Rabat": ("RAB", (34.0209, -6.8416),
              ["Agdal", "Hay Riad", "Souissi", "Ocean", "Hassan", "Kasbah des Oudayas", "Yacoub El Mansour", "Takaddoum", "Akkari", "Rabat Ville"],
              {"residentiel": 32, "centre_affaires": 18, "gare": 8, "universite": 14, "plage": 6, "tourisme": 7, "commercial": 15}),
    "Marrakech": ("MAR", (31.6295, -7.9811),
                  ["Gueliz", "Medina", "Hivernage", "Menara", "Palmeraie", "Majorelle", "Sidi Youssef Ben Ali", "Targa", "Massira", "Jemaa el-Fna"],
                  {"residentiel": 25, "centre_affaires": 12, "gare": 6, "universite": 8, "plage": 0, "tourisme": 34, "commercial": 15}),
    "Tanger": ("TAN", (35.7595, -5.8340),
               ["Malabata", "Marshan", "Medina", "Iberia", "Boukhalef", "Achakar", "Mesnana", "Branes", "Playa", "Tanger Ville"],
               {"residentiel": 30, "centre_affaires": 14, "gare": 8, "universite": 8, "plage": 20, "tourisme": 6, "commercial": 14}),
}
PER_CITY = 100


def main():
    stations = []
    for city, (prefix, (lat0, lon0), districts, weights) in CITIES.items():
        zones, w = list(weights), list(weights.values())
        for i in range(1, PER_CITY + 1):
            zone = random.choices(zones, weights=w)[0]
            district = random.choice(districts)
            stations.append({
                "station_id": f"S-{prefix}-{i:03d}",
                "name": f"{district} {zone.replace('_', ' ')} {i:03d}",
                "city": city,
                "district": district,
                "zone_type": zone,
                "capacity": random.choice([10, 15, 20, 25, 30]),
                "latitude": round(lat0 + random.uniform(-0.05, 0.05), 5),
                "longitude": round(lon0 + random.uniform(-0.06, 0.06), 5),
            })
    # un objet par ligne : fichier lisible et "400 lignes" comme dans le sujet
    OUT.write_text("[\n" + ",\n".join(json.dumps(s, ensure_ascii=False) for s in stations) + "\n]\n", encoding="utf8")
    print(f"{len(stations)} stations ecrites dans {OUT}")


if __name__ == "__main__":
    main()
