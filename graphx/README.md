# GraphX (Scala) - Personne 2

`GraphAnalysis.scala` : degres, PageRank (non pondere et pondere), composantes connexes, plus courts chemins.
Il lit les exports de `graphframes/graph_analysis.py` (`reports/graph_exports/`), donc a lancer **apres** lui.

Versions : Spark 3.5.3, Scala 2.12 (celle de Spark 3.5.3), Java 17.

## Cluster / Jupyter / Linux (Parquet)

Avec sbt (`build.sbt` a creer : `scalaVersion := "2.12.18"`, dependances `spark-sql` et `spark-graphx` 3.5.3
en `provided`) ; cette voie n'a pas ete executee, seule la compilation avec `scalac` 2.12.18 a ete verifiee :

```bash
sbt package
spark-submit --class GraphAnalysis --master spark://spark-master:7077 target/scala-2.12/*.jar reports/graph_exports
```

## Test local sous Windows (sans sbt ni winutils)

Spark ne peut pas lire/ecrire Parquet en local sous Windows sans `winutils`. On lit alors les CSV :

```bash
GRAPH_FORMAT=csv java <options --add-opens de Spark> -cp "<classes>;<jars pyspark>" GraphAnalysis reports/graph_exports
```

Le fichier a ete compile avec le `scalac` 2.12.18 embarque dans les jars de PySpark
(`scala.tools.nsc.Main`) et execute ainsi ; l'export final est alors ignore (ecriture Hadoop locale impossible).
