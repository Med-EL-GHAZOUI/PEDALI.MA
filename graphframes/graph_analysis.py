"""Partie 6 : analyse du reseau de stations avec GraphFrames (PySpark), a partir de la couche Silver.

  1. preparation : sommets = stations (id, ville, type de zone, capacite, coordonnees) ; aretes TRIP (station de depart ->
     station d'arrivee) agregees par couple de stations avec nombre de trajets, duree moyenne et distance moyenne, et une
     variante par periode (matin 6-10 h, journee 10-17 h, soir 17-6 h). Aretes orphelines exclues, boucles (depart = arrivee) a part.
  2. GraphFrame(vertices, edges) : nombres de sommets / aretes par ville, absence d'aretes orphelines
  3. motifs find() : allers-retours, boucles a 3 stations, corridors ; degres ponderes ; solde net par station et par periode
  4. export Parquet des memes tables (s3a://lake/gold/graph_export/) pour GraphX
  5. recommandation de reequilibrage (seuils explicites, couples station source -> station cible)
  6. resultats ecrits dans s3a://lake/gold/graph_analysis/ partitionne par city et result_type

Usage (Jupyter / cluster) : python graphframes/graph_analysis.py [--min-trips 3]
"""
import argparse
import json
import math
import sys
import time

sys.path.insert(0, "/home/jovyan/work/data")
sys.path.insert(0, "/home/jovyan/work/pipeline")
from graphframes import GraphFrame
from pedali_schema import read_stations
from pedali_spark import get_spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LAKE = "s3a://lake"
SILVER = f"{LAKE}/silver/trips_clean"
STATIONS = f"{LAKE}/bronze/history/stations.json"
EXPORT = f"{LAKE}/gold/graph_export"
ANALYSIS = f"{LAKE}/gold/graph_analysis"
PERIODS = ["matin", "journee", "soir"]


def period_of(hour):
    return (F.when((hour >= 6) & (hour < 10), "matin").when((hour >= 10) & (hour < 17), "journee").otherwise("soir"))


def prepare(spark: SparkSession):
    """Sommets, aretes agregees par couple, aretes par periode, boucles. Les aretes orphelines sont comptees puis exclues."""
    stations = read_stations(spark, STATIONS)
    vertices = stations.select(F.col("station_id").alias("id"), "name", "city", "district", "zone_type", "capacity", "latitude", "longitude")
    ended = (spark.read.parquet(SILVER).filter("status = 'ENDED' AND end_station IS NOT NULL")
             .withColumn("start_ts", F.to_timestamp(F.unix_timestamp("event_time") - F.col("duration_s")))
             .withColumn("period", period_of(F.hour("start_ts")))
             .select(F.col("start_station").alias("src"), F.col("end_station").alias("dst"), "period", "duration_s", "distance_km", "city"))

    ids = vertices.select(F.col("id").alias("sid"))
    orphans = ended.join(ids, ended.src == ids.sid, "left_anti").count() + ended.join(ids, ended.dst == ids.sid, "left_anti").count()
    known = (ended.join(ids.withColumnRenamed("sid", "s1"), ended.src == F.col("s1"), "left_semi")
             .join(ids.withColumnRenamed("sid", "s2"), F.col("dst") == F.col("s2"), "left_semi"))
    loops = known.filter("src = dst")
    trips = known.filter("src <> dst").cache()

    agg = [F.count("*").alias("trajets"), F.round(F.avg("duration_s"), 1).alias("duree_moy_s"), F.round(F.avg("distance_km"), 2).alias("distance_moy_km")]
    edges = trips.groupBy("src", "dst").agg(*agg)
    edges_period = trips.groupBy("src", "dst", "period").agg(*agg)
    return stations, vertices, edges, edges_period, loops, trips, orphans


def round_trips(g: GraphFrame) -> DataFrame:
    m = g.find("(a)-[e1]->(b); (b)-[e2]->(a)").filter("a.id < b.id")
    return m.select(F.col("a.id").alias("station_a"), F.col("b.id").alias("station_b"), F.col("a.city").alias("city"),
                    F.col("e1.trajets").alias("a_vers_b"), F.col("e2.trajets").alias("b_vers_a"),
                    (F.col("e1.trajets") + F.col("e2.trajets")).alias("total"))


def triangles(g: GraphFrame) -> DataFrame:
    m = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)").filter("a.id < b.id AND a.id < c.id AND b.id <> c.id")
    return m.select(F.col("a.id").alias("a"), F.col("b.id").alias("b"), F.col("c.id").alias("c"), F.col("a.city").alias("city"),
                    F.col("e1.trajets").alias("a_b"), F.col("e2.trajets").alias("b_c"), F.col("e3.trajets").alias("c_a"),
                    (F.col("e1.trajets") + F.col("e2.trajets") + F.col("e3.trajets")).alias("total"))


def degrees(g: GraphFrame) -> DataFrame:
    """Degres (voisins distincts) et degres ponderes par le nombre de trajets."""
    out_w = g.edges.groupBy(F.col("src").alias("id")).agg(F.sum("trajets").alias("departs"))
    in_w = g.edges.groupBy(F.col("dst").alias("id")).agg(F.sum("trajets").alias("arrivees"))
    return (g.vertices.select("id", "name", "city", "zone_type", "capacity")
            .join(g.outDegrees.withColumnRenamed("outDegree", "voisins_sortants"), "id", "left")
            .join(g.inDegrees.withColumnRenamed("inDegree", "voisins_entrants"), "id", "left")
            .join(out_w, "id", "left").join(in_w, "id", "left").na.fill(0))


def net_balance(edges_period: DataFrame, vertices: DataFrame, days: int, by_period: bool) -> DataFrame:
    """Solde net = arrivees - departs (par jour en moyenne). Negatif : la station se vide ; positif : elle sature."""
    dep = edges_period.select(F.col("src").alias("id"), "period", (-F.col("trajets")).alias("delta"))
    arr = edges_period.select(F.col("dst").alias("id"), "period", F.col("trajets").alias("delta"))
    keys = ["id", "period"] if by_period else ["id"]
    bal = dep.unionByName(arr).groupBy(*keys).agg((F.sum("delta") / days).alias("solde_jour"))
    return (bal.join(vertices.select("id", "name", "city", "zone_type", "capacity"), "id")
            .withColumn("ratio_capacite", F.round(F.abs("solde_jour") / F.col("capacity"), 3))
            .withColumn("solde_jour", F.round("solde_jour", 2)))


def haversine(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = 0.5 - math.cos((lat2 - lat1) * p) / 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * (1 - math.cos((lon2 - lon1) * p)) / 2
    return 12742 * math.asin(math.sqrt(a))


def recommend(bal, vertices, alert: float, urgent: float):
    """Stations a reapprovisionner (solde negatif) -> station cible la plus proche ayant un excedent (solde positif), meme ville."""
    import pandas as pd
    pdf = bal.toPandas()
    pdf["niveau"] = pdf["ratio_capacite"].apply(lambda r: "urgence" if r >= urgent else "alerte" if r >= alert else "ok")
    sources = pdf[(pdf.solde_jour < 0) & (pdf.niveau != "ok")].sort_values("ratio_capacite", ascending=False)
    sinks = pdf[(pdf.solde_jour > 0)]
    coords = {r.id: (r.latitude, r.longitude) for r in vertices.collect()}
    rows = []
    for _, s in sources.iterrows():
        cands = sinks[(sinks.city == s.city) & (sinks.solde_jour > 0)]
        if cands.empty:
            continue
        d = cands.apply(lambda c: haversine(*coords[s.id], *coords[c.id]), axis=1)
        t = cands.assign(distance_km=d).sort_values(["distance_km", "solde_jour"], ascending=[True, False]).iloc[0]
        rows.append({"source": s.id, "cible": t.id, "city": s.city, "niveau": s.niveau, "solde_source_jour": float(s.solde_jour),
                     "ratio_capacite": float(s.ratio_capacite), "solde_cible_jour": float(t.solde_jour), "distance_km": round(float(t.distance_km), 2)})
    return pd.DataFrame(rows)


def long_format(spark, parts: dict) -> DataFrame:
    """Schema commun : result_type, city, entity, value, detail (JSON)."""
    dfs = []
    for result_type, (df, entity, value, detail_cols) in parts.items():
        dfs.append(df.select(F.lit(result_type).alias("result_type"), "city", entity.alias("entity"), F.col(value).cast("double").alias("value"),
                             (F.to_json(F.struct(*[F.col(c) for c in detail_cols])) if detail_cols else F.lit("{}")).alias("detail")))
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-trips", type=int, default=3, help="seuil de trajets par arete pour les motifs")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--alert", type=float, default=0.30, help="alerte : |solde net journalier| / capacite")
    ap.add_argument("--urgent", type=float, default=0.50, help="urgence : |solde net journalier| / capacite")
    args = ap.parse_args()

    spark = get_spark("pedali-graphframes")
    spark.sparkContext.setLogLevel("ERROR")
    t0 = time.time()
    stations, vertices, edges, edges_period, loops, trips, orphans = prepare(spark)
    vertices.cache(); edges.cache(); edges_period.cache()

    g = GraphFrame(vertices, edges)
    print(f"Graphe : {g.vertices.count()} sommets, {g.edges.count()} aretes (couples de stations), {edges_period.count()} aretes par periode")
    print(f"Boucles (depart = arrivee) mises a part : {loops.count()} trajets ; aretes orphelines : {orphans}")
    print("\n== Sommets et aretes par ville ==")
    per_city = (g.vertices.groupBy("city").agg(F.count("*").alias("sommets"))
                .join(g.edges.join(g.vertices.select(F.col("id").alias("src"), "city"), "src").groupBy("city")
                      .agg(F.count("*").alias("aretes"), F.sum("trajets").alias("trajets")), "city").orderBy("city"))
    per_city.show()
    assert g.edges.join(g.vertices.select(F.col("id").alias("src")), "src", "left_anti").count() == 0, "aretes orphelines (source)"
    assert g.edges.join(g.vertices.select(F.col("id").alias("dst")), "dst", "left_anti").count() == 0, "aretes orphelines (destination)"
    print("verification : aucune arete orpheline")
    print(f"preparation + comptages : {time.time() - t0:.1f} s")

    # ---- export Parquet pour GraphX (memes tables)
    vertices.write.mode("overwrite").parquet(f"{EXPORT}/sommets")
    edges.write.mode("overwrite").parquet(f"{EXPORT}/aretes")
    edges_period.write.mode("overwrite").parquet(f"{EXPORT}/aretes_periode")
    print(f"export Parquet : {EXPORT}/(sommets|aretes|aretes_periode)")

    # ---- motifs (aretes >= seuil)
    t1 = time.time()
    gm = GraphFrame(vertices, edges.filter(F.col("trajets") >= args.min_trips))
    cor = (edges.filter(F.col("trajets") >= args.min_trips).join(vertices.select(F.col("id").alias("src"), "city"), "src")
           .withColumn("corridor", F.concat_ws("->", "src", "dst")).orderBy(F.desc("trajets")))
    print(f"\n== Corridors les plus frequentes (>= {args.min_trips} trajets) ==")
    cor.select("corridor", "city", "trajets", "duree_moy_s", "distance_moy_km").show(10, truncate=False)
    rt = round_trips(gm).orderBy(F.desc("total")).cache()
    print(f"== Allers-retours A->B et B->A (aretes >= {args.min_trips}) : {rt.count()} paires ==")
    rt.show(10, truncate=False)
    tri = triangles(gm).orderBy(F.desc("total")).cache()
    print(f"== Boucles a trois stations A->B->C->A (aretes >= {args.min_trips}) : {tri.count()} boucles ==")
    tri.show(10, truncate=False)
    print(f"motifs : {time.time() - t1:.1f} s")

    # ---- degres, PageRank GraphFrames
    deg = degrees(g).cache()
    print("== Degres : 10 stations avec le plus de departs ==")
    deg.orderBy(F.desc("departs")).show(10, truncate=False)
    t2 = time.time()
    pr = g.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "city", F.col("pagerank").alias("rank"))
    pr.cache().count()
    print(f"PageRank GraphFrames (non pondere, 10 iterations) : {time.time() - t2:.1f} s")
    pr.orderBy(F.desc("rank")).show(5)

    # ---- soldes nets
    bal = net_balance(edges_period, vertices, args.days, by_period=False).cache()
    bal_p = net_balance(edges_period, vertices, args.days, by_period=True).cache()
    print("== Soldes nets journaliers : stations qui se vident / saturent (rapport a la capacite) ==")
    bal.orderBy("solde_jour").show(8, truncate=False)
    bal.orderBy(F.desc("solde_jour")).show(5, truncate=False)
    print("== Solde net journalier par type de zone et par periode (flux pendulaires) ==")
    zone = (edges_period.join(vertices.select(F.col("id").alias("src"), F.col("zone_type").alias("zs")), "src")
            .join(vertices.select(F.col("id").alias("dst"), F.col("zone_type").alias("zd")), "dst"))
    dep_z = zone.groupBy(F.col("zs").alias("zone_type"), "period").agg((-F.sum("trajets") / args.days).alias("d"))
    arr_z = zone.groupBy(F.col("zd").alias("zone_type"), "period").agg((F.sum("trajets") / args.days).alias("a"))
    zb = (dep_z.join(arr_z, ["zone_type", "period"]).withColumn("solde_jour", F.round(F.col("d") + F.col("a"), 1))
          .select("zone_type", "period", "solde_jour"))
    piv = zb.groupBy("zone_type").pivot("period", PERIODS).agg(F.first("solde_jour")).orderBy("zone_type")
    piv.show(truncate=False)

    # ---- recommandation
    rec = recommend(bal, vertices, args.alert, args.urgent)
    rec_sdf = spark.createDataFrame(rec) if len(rec) else None
    print(f"== Recommandations de reequilibrage (alerte >= {args.alert:.0%}, urgence >= {args.urgent:.0%} de la capacite par jour) : {len(rec)} couples ==")
    print(rec.sort_values("ratio_capacite", ascending=False).head(15).to_string(index=False) if len(rec) else "aucun")

    # ---- ecriture dans gold/graph_analysis (city x result_type)
    parts = {
        "degres": (deg, deg.id, "departs", ["voisins_sortants", "voisins_entrants", "departs", "arrivees", "zone_type"]),
        "pagerank_graphframes": (pr, pr.id, "rank", []),
        "soldes_nets": (bal, bal.id, "solde_jour", ["ratio_capacite", "capacity", "zone_type"]),
        "soldes_nets_periode": (bal_p, F.concat_ws("|", bal_p.id, bal_p.period), "solde_jour", ["period", "ratio_capacite", "capacity"]),
        "corridors": (cor.limit(200), F.col("corridor"), "trajets", ["duree_moy_s", "distance_moy_km"]),
        "allers_retours": (rt, F.concat_ws("<->", rt.station_a, rt.station_b), "total", ["a_vers_b", "b_vers_a"]),
        "boucles_3": (tri, F.concat_ws("->", tri.a, tri.b, tri.c, tri.a), "total", ["a_b", "b_c", "c_a"]),
    }
    if rec_sdf is not None:
        parts["recommandations"] = (rec_sdf, F.concat_ws("->", rec_sdf.source, rec_sdf.cible), "ratio_capacite",
                                    ["niveau", "solde_source_jour", "solde_cible_jour", "distance_km"])
    out = long_format(spark, parts)
    out.write.mode("overwrite").partitionBy("city", "result_type").parquet(ANALYSIS)
    print(f"\nResultats ecrits dans {ANALYSIS} (partitionne par city et result_type) : {spark.read.parquet(ANALYSIS).count()} lignes")
    print(f"duree totale : {time.time() - t0:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
