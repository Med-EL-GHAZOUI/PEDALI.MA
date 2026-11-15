"""KPI Pedali (Personne 2) : definitions + calcul Spark.

Definitions (voir docs/schema_donnees.md, regle 6) :
  - trajets termines      : status = completed
  - chiffre d'affaires    : somme de price_mad des trajets termines
  - duree moyenne         : moyenne de duration_min des trajets termines
  - taux de panne         : trajets breakdown   / tous les trajets valides
  - taux d'annulation     : trajets cancelled   / tous les trajets valides
"Valides" = apres nettoyage (clean_trips), avant filtre sur le statut.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _kpi_aggs():
    done = F.col("status") == "completed"
    return [
        F.count("*").alias("trajets_valides"),
        F.sum(done.cast("int")).alias("trajets_termines"),
        F.round(F.sum(F.when(done, F.col("price_mad"))), 2).alias("chiffre_affaires"),
        F.round(F.avg(F.when(done, F.col("duration_min"))), 2).alias("duree_moyenne_min"),
        F.round(F.avg((F.col("status") == "breakdown").cast("int")), 4).alias("taux_panne"),
        F.round(F.avg((F.col("status") == "cancelled").cast("int")), 4).alias("taux_annulation"),
    ]


def kpis_global(valid_trips: DataFrame) -> DataFrame:
    return valid_trips.agg(*_kpi_aggs())


def kpis_by(valid_trips: DataFrame, *cols: str) -> DataFrame:
    return valid_trips.groupBy(*cols).agg(*_kpi_aggs()).orderBy(*cols)


def kpis_by_window(valid_trips: DataFrame, duration: str = "1 hour") -> DataFrame:
    """KPI par fenetre temporelle sur start_time (utile en batch ; en streaming ajouter withWatermark)."""
    return (valid_trips.groupBy(F.window("start_time", duration).alias("fenetre"))
            .agg(*_kpi_aggs())
            .select(F.col("fenetre.start").alias("debut"), F.col("fenetre.end").alias("fin"), "*")
            .drop("fenetre").orderBy("debut"))
