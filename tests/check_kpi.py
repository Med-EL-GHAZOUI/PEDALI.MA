"""Verifie les KPI Spark contre un calcul Python pur independant.
Usage : python tests/check_kpi.py   (depuis la racine du depot ; pyspark requis)
"""
import json, os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "data"))
from pyspark.sql import SparkSession
from pedali_kpi import kpis_by, kpis_by_window, kpis_global
from pedali_schema import clean_trips, read_stations, read_trips

spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]")).appName("check-kpi")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
stations = read_stations(spark, str(ROOT / "data/stations.json"))
valid = clean_trips(read_trips(spark, str(ROOT / "data/trips_history.jsonl")), stations).cache()

# --- reference Python pur
st = {s["station_id"] for s in json.loads((ROOT / "data/stations.json").read_text(encoding="utf8"))}
seen, rows = set(), []
for line in (ROOT / "data/trips_history.jsonl").read_text(encoding="utf8").splitlines():
    t = json.loads(line)
    if t["trip_id"] in seen or not t["user_id"] or t["start_station_id"] not in st:
        continue
    if t["duration_min"] is not None and t["duration_min"] < 0:
        continue
    seen.add(t["trip_id"]); rows.append(t)
done = [t for t in rows if t["status"] == "completed"]
ref = {
    "trajets_valides": len(rows),
    "trajets_termines": len(done),
    "chiffre_affaires": round(sum(t["price_mad"] for t in done), 2),
    "duree_moyenne_min": round(sum(t["duration_min"] for t in done) / len(done), 2),
    "taux_panne": round(sum(t["status"] == "breakdown" for t in rows) / len(rows), 4),
    "taux_annulation": round(sum(t["status"] == "cancelled" for t in rows) / len(rows), 4),
}
got = kpis_global(valid).first().asDict()
print("reference:", ref); print("spark    :", got)
assert got == ref, "KPI globaux differents"

# --- coherence : somme des KPI par ville == global ; somme des fenetres == global
by_city = kpis_by(valid, "city").collect()
assert sum(r.trajets_termines for r in by_city) == ref["trajets_termines"]
assert round(sum(r.chiffre_affaires for r in by_city), 2) == ref["chiffre_affaires"]
win = kpis_by_window(valid, "1 day").collect()
assert sum(r.trajets_valides for r in win) == ref["trajets_valides"]
print("OK : KPI Spark == Python pur ; ville et fenetres coherentes")
spark.stop()
