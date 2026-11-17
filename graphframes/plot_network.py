"""Partie 6 : representation visuelle du reseau d'une ville.

Taille des sommets proportionnelle au PageRank (GraphX, pondere par les trajets), epaisseur des aretes proportionnelle
au nombre de trajets, couleur des sommets = solde net journalier (rouge : la station se vide, bleu : elle sature).
Les stations a reequilibrer (recommandations) sont entourees et annotees.

Usage (Jupyter) : python graphframes/plot_network.py [Marrakech Casablanca ...]
Sortie : reports/graph_exports/reseau_<ville>.png
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/home/jovyan/work/pipeline")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from pedali_spark import get_spark
from pyspark.sql import functions as F

LAKE = "s3a://lake"
OUT = Path("/home/jovyan/work/reports/graph_exports")


def plot_city(city: str, vertices, edges, analysis, min_trips: int):
    v = vertices[vertices.city == city].set_index("id")
    e = edges[(edges.src.isin(v.index)) & (edges.trajets >= min_trips)]
    pr = analysis[(analysis.result_type == "pagerank_graphx") & (analysis.city == city)].set_index("entity")["value"]
    bal = analysis[(analysis.result_type == "soldes_nets") & (analysis.city == city)].set_index("entity")["value"]
    rec = analysis[(analysis.result_type == "recommandations") & (analysis.city == city)]

    fig, ax = plt.subplots(figsize=(11, 8.5))
    maxw = max(float(e.trajets.max()), 1.0) if len(e) else 1.0
    for _, r in e.iterrows():
        a, b = v.loc[r.src], v.loc[r.dst]
        ax.annotate("", xy=(b.longitude, b.latitude), xytext=(a.longitude, a.latitude),
                    arrowprops=dict(arrowstyle="-|>", lw=0.4 + 5.5 * r.trajets / maxw, alpha=0.35 + 0.5 * r.trajets / maxw,
                                    color="#555555", shrinkA=4, shrinkB=4, connectionstyle="arc3,rad=0.12"))
    sizes = [30 + 260 * (pr.get(i, 1.0) / max(pr.max(), 1e-9)) for i in v.index]
    colors = [bal.get(i, 0.0) for i in v.index]
    lim = max(abs(min(colors)), abs(max(colors)), 1e-9)
    sc = ax.scatter(v.longitude, v.latitude, s=sizes, c=colors, cmap="coolwarm_r", vmin=-lim, vmax=lim,
                    edgecolors="black", linewidths=0.4, zorder=3)
    for sid in set(rec.entity.str.split("->").str[0]):
        if sid in v.index:
            ax.scatter([v.loc[sid].longitude], [v.loc[sid].latitude], s=520, facecolors="none", edgecolors="#c0392b", linewidths=2, zorder=4)
            ax.annotate(sid, (v.loc[sid].longitude, v.loc[sid].latitude), xytext=(6, 8), textcoords="offset points", fontsize=8, color="#c0392b", weight="bold")
    for sid in pr.sort_values(ascending=False).head(5).index:
        if sid in v.index:
            ax.annotate(sid, (v.loc[sid].longitude, v.loc[sid].latitude), xytext=(5, -11), textcoords="offset points", fontsize=7)
    cb = fig.colorbar(sc, ax=ax, shrink=0.7)
    cb.set_label("solde net journalier (arrivees - departs) : rouge = se vide, bleu = sature")
    ax.set_title(f"Reseau de stations - {city} (aretes >= {min_trips} trajets)\n"
                 f"sommet : taille = PageRank GraphX pondere ; arete : epaisseur = nombre de trajets\n"
                 f"cercle rouge = station a reequilibrer", fontsize=10)
    ax.set_xlabel("longitude"); ax.set_ylabel("latitude"); ax.grid(alpha=0.2)
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"reseau_{city.lower()}.png"
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)
    print("ecrit :", path)


def main():
    cities = sys.argv[1:] or ["Marrakech", "Casablanca"]
    spark = get_spark("pedali-plot")
    spark.sparkContext.setLogLevel("ERROR")
    vertices = spark.read.parquet(f"{LAKE}/gold/graph_export/sommets").toPandas()
    edges = spark.read.parquet(f"{LAKE}/gold/graph_export/aretes").toPandas()
    analysis = spark.read.parquet(f"{LAKE}/gold/graph_analysis").toPandas()
    spark.stop()
    for city in cities:
        plot_city(city, vertices, edges, analysis, min_trips=3)


if __name__ == "__main__":
    main()
