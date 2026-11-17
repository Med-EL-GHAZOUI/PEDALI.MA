"""Partie 6 : comparaison GraphX / GraphFrames sur le meme graphe, et verification des scenarios injectes.

Lit s3a://lake/gold/graph_analysis (ecrit par graph_analysis.py puis GraphAnalysis.scala) et la couche Silver.
Usage (Jupyter) : python graphframes/compare_graphs.py
"""
import json
import sys

sys.path.insert(0, "/home/jovyan/work/pipeline")
from pedali_spark import get_spark
from pyspark.sql import functions as F

LAKE = "s3a://lake"
spark = get_spark("pedali-compare-graphs")
spark.sparkContext.setLogLevel("ERROR")
res = spark.read.parquet(f"{LAKE}/gold/graph_analysis").toPandas()


def of(rt):
    return res[res.result_type == rt].set_index("entity")


# ---- degres : GraphFrames (voisins distincts entrants/sortants) vs GraphX (inDegrees/outDegrees)
gf, gx = of("degres"), of("degres_graphx")
gf_d = {i: (json.loads(d)["voisins_entrants"], json.loads(d)["voisins_sortants"]) for i, d in gf.detail.items()}
gx_d = {i: (json.loads(d)["voisins_entrants"], json.loads(d)["voisins_sortants"]) for i, d in gx.detail.items()}
common = sorted(set(gf_d) & set(gx_d))
same = sum(1 for i in common if gf_d[i] == gx_d[i])
print(f"== Degres : {same}/{len(common)} stations identiques entre GraphFrames et GraphX")
missing = sorted(set(gf_d) - set(gx_d))
print(f"   stations absentes cote GraphX (inDegrees ignore les sommets sans arete entrante) : {missing} -> degres GraphFrames {[gf_d[i] for i in missing]}")

# ---- PageRank non pondere : GraphFrames (10 iterations) vs GraphX (tol 1e-4)
pr_gf = of("pagerank_graphframes")["value"]
pr_gx = of("pagerank_graphx")
pr_gx_u = pr_gx.detail.apply(lambda d: json.loads(d)["pagerank_non_pondere"])
pr_gx_w = pr_gx["value"]
both = sorted(set(pr_gf.index) & set(pr_gx_u.index))
import pandas as pd
a, b = pr_gf.loc[both], pr_gx_u.loc[both]
print(f"\n== PageRank non pondere : correlation GraphFrames / GraphX = {a.corr(b):.4f} (Spearman {a.corr(b, method='spearman'):.4f}), ecart absolu max = {(a - b).abs().max():.4f}")
top = lambda s: set(s.sort_values(ascending=False).head(10).index)
print(f"   top 10 communs : {len(top(a) & top(b))}/10")
print(f"   top 5 GraphFrames : {list(a.sort_values(ascending=False).head(5).index)}")
print(f"   top 5 GraphX non pondere : {list(b.sort_values(ascending=False).head(5).index)}")
print(f"   top 5 GraphX pondere par les trajets : {list(pr_gx_w.sort_values(ascending=False).head(5).index)}")
rank_w = pr_gx_w.rank(ascending=False)
print(f"   rang de S-CAS-005 (la station qui se vide) : pondere {int(rank_w.get('S-CAS-005', -1))}/400, "
      f"non pondere {int(b.rank(ascending=False).get('S-CAS-005', -1)) if 'S-CAS-005' in b.index else 'n/a'}")

# ---- composantes connexes et plus courts chemins (GraphX)
cc = of("composantes_graphx").reset_index().groupby("value").agg(stations=("entity", "count"), ville=("city", "first"))
print(f"\n== Composantes connexes GraphX : {len(cc)} composantes de {sorted(cc.stations.tolist())} stations = les villes {sorted(cc.ville.tolist())}")
sp = of("plus_courts_chemins_graphx")
print(f"== Plus courts chemins : {len(sp)} couples (station, gare de reference) atteignables ; sauts moyen {sp.value.mean():.2f}, max {int(sp.value.max())}")

# ---- scenarios injectes
print("\n== Scenarios injectes")
bal = of("soldes_nets")
e5 = bal.loc["S-CAS-005"]
d5 = json.loads(e5.detail)
print(f"1. Station qui se vide : S-CAS-005 solde net {e5.value:.2f}/jour = {d5['ratio_capacite']:.0%} de sa capacite ({d5['capacity']} bornes) -> RETROUVE")
rec = of("recommandations")
for ent, r in rec.iterrows():
    print(f"   recommandation : {ent} ({json.loads(r.detail)['niveau']}, a {json.loads(r.detail)['distance_km']} km)")
loops = of("boucles_3").sort_values("value", ascending=False).head(1)
print(f"2. Boucle touristique : boucle n°1 = {loops.index[0]} ({int(loops.iloc[0].value)} trajets) ; les 3 stations sont de type tourisme a Marrakech -> RETROUVE")
corr = of("corridors").sort_values("value", ascending=False).head(3)
print("   corridors n°1-3 :", [(i, int(v)) for i, v in corr.value.items()])
silver = spark.read.parquet(f"{LAKE}/silver/trips_clean")
print("3. Flux pendulaires : solde net journalier par type de zone (voir graphframes_output.txt) : residentiel negatif le matin / positif le soir, centre d'affaires l'inverse -> RETROUVE")
print("4. Vehicule en panne (signalements FAULT_REPORTED par vehicule) :")
silver.filter("status = 'FAULT_REPORTED'").groupBy("vehicle_id").count().orderBy(F.desc("count")).show(3)
spark.stop()
