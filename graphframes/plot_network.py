"""Visualisation du reseau de stations (Personne 2).

Une carte par ville : position = lat/lon, epaisseur de l'arete = nombre de trajets,
taille du noeud = capacite, couleur = solde net (rouge = se vide, bleu = se remplit).

Usage : python graphframes/plot_network.py   (apres graph_analysis.py ; pandas + matplotlib requis)
Sortie : screenshots/reseau_stations.png
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyArrowPatch

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "reports/graph_exports/csv"

stations = pd.read_csv(CSV / "sommets.csv").set_index("id")
edges = pd.read_csv(CSV / "aretes.csv")
balance = pd.read_csv(CSV / "soldes_nets.csv").set_index("id")["solde_net"]
stations["solde"] = balance
vmax = balance.abs().max()

cities = sorted(stations.city.unique())
fig, axes = plt.subplots(1, len(cities), figsize=(6 * len(cities), 5.5))
max_trips = edges.trajets.max()

for ax, city in zip(axes, cities):
    st = stations[stations.city == city]
    ax.set_title(city, fontsize=14, weight="bold")
    for _, e in edges[edges.src.isin(st.index) & edges.dst.isin(st.index) & (edges.src != edges.dst)].iterrows():
        a, b = st.loc[e.src], st.loc[e.dst]
        arrow = FancyArrowPatch((a.lon, a.lat), (b.lon, b.lat), connectionstyle="arc3,rad=0.18",
                                arrowstyle="-|>", mutation_scale=12, shrinkA=14, shrinkB=14,
                                linewidth=0.6 + 5.5 * e.trajets / max_trips, color="#555", alpha=0.35 + 0.5 * e.trajets / max_trips)
        ax.add_patch(arrow)
    sc = ax.scatter(st.lon, st.lat, s=st.capacity * 25, c=st.solde, cmap="coolwarm_r", vmin=-vmax, vmax=vmax,
                    edgecolors="black", zorder=3)
    for sid, r in st.iterrows():
        ax.annotate(f"{sid}\n{r['name']}", (r.lon, r.lat), textcoords="offset points", xytext=(0, 16),
                    ha="center", fontsize=8)
    pad_x = (st.lon.max() - st.lon.min()) * 0.25 + 1e-3
    pad_y = (st.lat.max() - st.lat.min()) * 0.25 + 1e-3
    ax.set_xlim(st.lon.min() - pad_x, st.lon.max() + pad_x)
    ax.set_ylim(st.lat.min() - pad_y, st.lat.max() + pad_y)
    ax.set_xticks([])
    ax.set_yticks([])

cbar = fig.colorbar(sc, ax=axes, shrink=0.8, pad=0.02)
cbar.set_label("solde net (arrivees - departs) : rouge = se vide, bleu = se remplit")
fig.suptitle("Reseau Pedali.ma - trajets entre stations (octobre 2026)", fontsize=15)
out = ROOT / "screenshots" / "reseau_stations.png"
out.parent.mkdir(exist_ok=True)
fig.savefig(out, dpi=130, bbox_inches="tight")
print("ecrit :", out)
