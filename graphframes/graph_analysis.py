"""Partie 6 - Analyse du reseau de stations avec GraphFrames (Personne 2).

Sommets = stations ; aretes = trajets termines (start_station_id -> end_station_id).
Les donnees viennent du referentiel + trajets nettoyes (equivalent de la couche Silver ;
brancher `load_silver()` sur s3a://lake/silver/ une fois la Partie 5 de la Personne 1 prete).

Usage (local) :
    python graphframes/graph_analysis.py [--out reports/graph_exports]
Cluster / Jupyter : spark-submit --packages io.graphframes:graphframes-spark3_2.12:0.10.0 ...
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "data"))

from graphframes import GraphFrame
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pedali_schema import clean_trips, read_stations, read_trips

GRAPHFRAMES_PACKAGE = "io.graphframes:graphframes-spark3_2.12:0.10.0"


def build_spark() -> SparkSession:
    """GRAPHFRAMES_JARS (liste de jars separes par `;`) : test local sous Windows, ou `--packages` est
    inutilisable sans winutils. Sinon le package Maven est resolu automatiquement (cluster, Jupyter)."""
    builder = (SparkSession.builder.appName("pedali-graphframes")
               .master(os.environ.get("SPARK_MASTER", "local[4]"))
               .config("spark.sql.shuffle.partitions", "8"))
    local_jars = os.environ.get("GRAPHFRAMES_JARS")
    if local_jars:
        builder = builder.config("spark.driver.extraClassPath", local_jars)
    else:
        builder = builder.config("spark.jars.packages", GRAPHFRAMES_PACKAGE)
    return builder.getOrCreate()


def load_silver(spark: SparkSession):
    """Stations + trajets valides et termines (avec arrivee connue)."""
    stations = read_stations(spark, str(ROOT / "data/stations.json"))
    trips = clean_trips(read_trips(spark, str(ROOT / "data/trips_history.jsonl")), stations)
    done = trips.filter((F.col("status") == "completed") & F.col("end_station_id").isNotNull())
    return stations, done


def build_graph(stations: DataFrame, trips: DataFrame) -> GraphFrame:
    """GraphFrame dont les aretes sont agregees par paire de stations (nombre de trajets, revenu)."""
    vertices = stations.withColumnRenamed("station_id", "id")
    edges = (trips.groupBy(F.col("start_station_id").alias("src"), F.col("end_station_id").alias("dst"))
             .agg(F.count("*").alias("trajets"), F.round(F.sum("price_mad"), 2).alias("revenu")))
    return GraphFrame(vertices, edges)


def round_trips(g: GraphFrame, min_trips: int) -> DataFrame:
    """Allers-retours A->B et B->A (chaque paire une seule fois)."""
    m = g.find("(a)-[e1]->(b); (b)-[e2]->(a)").filter("a.id < b.id")
    m = m.filter((F.col("e1.trajets") >= min_trips) & (F.col("e2.trajets") >= min_trips))
    return m.select(F.col("a.id").alias("station_a"), F.col("b.id").alias("station_b"),
                    F.col("e1.trajets").alias("a_vers_b"), F.col("e2.trajets").alias("b_vers_a"),
                    (F.col("e1.trajets") + F.col("e2.trajets")).alias("total"))


def triangles(g: GraphFrame, min_trips: int) -> DataFrame:
    """Boucles a trois stations A->B->C->A (une seule rotation : A est la plus petite)."""
    m = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
    m = m.filter("a.id < b.id AND a.id < c.id AND b.id <> c.id")
    m = m.filter((F.col("e1.trajets") >= min_trips) & (F.col("e2.trajets") >= min_trips)
                 & (F.col("e3.trajets") >= min_trips))
    return m.select(F.col("a.id").alias("a"), F.col("b.id").alias("b"), F.col("c.id").alias("c"),
                    F.col("e1.trajets").alias("a_b"), F.col("e2.trajets").alias("b_c"), F.col("e3.trajets").alias("c_a"),
                    (F.col("e1.trajets") + F.col("e2.trajets") + F.col("e3.trajets")).alias("total"))


def corridors(g: GraphFrame, top: int = 10) -> DataFrame:
    return g.edges.filter("src <> dst").orderBy(F.desc("trajets")).limit(top)


def degrees(g: GraphFrame) -> DataFrame:
    """Degres (voisins distincts) et degres ponderes (nombre de trajets) par station."""
    out_w = g.edges.groupBy(F.col("src").alias("id")).agg(F.sum("trajets").alias("departs"))
    in_w = g.edges.groupBy(F.col("dst").alias("id")).agg(F.sum("trajets").alias("arrivees"))
    return (g.vertices.select("id", "name", "city")
            .join(g.outDegrees.withColumnRenamed("outDegree", "voisins_sortants"), "id", "left")
            .join(g.inDegrees.withColumnRenamed("inDegree", "voisins_entrants"), "id", "left")
            .join(out_w, "id", "left").join(in_w, "id", "left")
            .na.fill(0).orderBy("id"))


def net_balance(trips: DataFrame, by_period: bool) -> DataFrame:
    """Solde net = arrivees - departs. Negatif : la station se vide ; positif : elle se remplit."""
    dep = trips.select(F.col("start_station_id").alias("id"), F.col("start_time").alias("t"), F.lit(-1).alias("delta"))
    arr = trips.select(F.col("end_station_id").alias("id"), F.col("end_time").alias("t"), F.lit(1).alias("delta"))
    moves = dep.unionByName(arr)
    if not by_period:
        return moves.groupBy("id").agg(F.sum("delta").alias("solde_net")).orderBy("solde_net")
    h = F.hour("t")
    periode = (F.when(h.between(6, 9), "1_matin (6-10h)").when(h.between(10, 15), "2_journee (10-16h)")
               .when(h.between(16, 20), "3_soir (16-21h)").otherwise("4_nuit (21-6h)"))
    return moves.groupBy("id", periode.alias("periode")).agg(F.sum("delta").alias("solde_net")).orderBy("id", "periode")


def sources_and_sinks(balance: DataFrame, stations: DataFrame, ratio: float = 0.5) -> DataFrame:
    """Source = solde fortement negatif, puits = fortement positif.

    Seuil = `ratio` x l'ecart moyen |solde| des stations (donc relatif a l'activite du reseau).
    """
    mean_abs = balance.agg(F.avg(F.abs("solde_net"))).first()[0] or 1
    seuil = ratio * mean_abs
    return (balance.join(stations.select(F.col("station_id").alias("id"), "name", "city", "capacity"), "id")
            .withColumn("role", F.when(F.col("solde_net") <= -seuil, "SOURCE (se vide)")
                                 .when(F.col("solde_net") >= seuil, "PUITS (sature)")
                                 .otherwise("equilibree")))


def export_parquet(df: DataFrame, path: Path) -> None:
    """Ecrit `df` en Parquet (un dossier `path`).

    Windows sans HADOOP_HOME (pas de winutils) : l'ecriture Spark echoue, on passe par pandas/pyarrow
    (les resultats sont petits). Ailleurs (Linux, cluster, s3a://) : ecriture Spark classique.
    """
    if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
        path.mkdir(parents=True, exist_ok=True)
        df.toPandas().to_parquet(path / "part-0.parquet", index=False)
    else:
        df.coalesce(1).write.mode("overwrite").parquet(str(path))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "reports/graph_exports"))
    ap.add_argument("--min-trips", type=int, default=50, help="seuil de trajets pour un motif")
    args = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    stations, trips = load_silver(spark)
    trips.cache()
    g = build_graph(stations, trips)
    g = GraphFrame(g.vertices.cache(), g.edges.cache())
    print(f"Graphe : {g.vertices.count()} sommets, {g.edges.count()} aretes (paires de stations)")

    print("\n== Corridors les plus frequentes ==")
    cor = corridors(g)
    cor.show()
    print("== Allers-retours ==")
    rt = round_trips(g, args.min_trips).orderBy(F.desc("total"))
    rt.show()
    print("== Boucles a trois stations ==")
    tri = triangles(g, args.min_trips).orderBy(F.desc("total"))
    tri.show()
    print("== Degres ==")
    deg = degrees(g)
    deg.show(20)
    print("== Solde net global ==")
    bal = net_balance(trips, by_period=False)
    roles = sources_and_sinks(bal, stations).orderBy("solde_net")
    roles.show(20, truncate=False)
    print("== Solde net par periode ==")
    bal_p = net_balance(trips, by_period=True)
    bal_p.show(60)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    exports = {"corridors": cor, "allers_retours": rt, "boucles_3": tri, "degres": deg,
               "soldes_nets": roles, "soldes_nets_periode": bal_p,
               "sommets": g.vertices, "aretes": g.edges}
    (out / "csv").mkdir(exist_ok=True)
    for name, df in exports.items():
        export_parquet(df, out / name)
        df.toPandas().to_csv(out / "csv" / f"{name}.csv", index=False)  # lisible par GraphX local, Gephi, Excel
    print(f"\nExports Parquet ecrits dans {out}")
    spark.stop()


if __name__ == "__main__":
    main()
