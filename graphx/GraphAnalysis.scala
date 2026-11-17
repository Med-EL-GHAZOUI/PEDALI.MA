// Partie 6 - Analyse du reseau de stations avec GraphX (Personne 2).
//
// Entree : exports Parquet de graphframes/graph_analysis.py (sommets, aretes), pour que GraphX et
// GraphFrames travaillent sur exactement le meme graphe. Sommets = stations, aretes = trajets
// termines agreges par paire (attribut = nombre de trajets).
//
// Compilation / execution : voir graphx/README.md (Spark 3.5.3, Scala 2.12).
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession

object GraphAnalysis {

  // "S07" -> 7L : GraphX exige des identifiants Long.
  private def vertexId(stationId: String): Long = stationId.stripPrefix("S").toLong
  private def stationId(vid: Long): String = f"S$vid%02d"

  // PageRank pondere par le nombre de trajets (graph.pageRank de GraphX ignore l'attribut des aretes).
  // Meme convention que GraphX : rang non normalise, la somme vaut le nombre de sommets.
  private def weightedPageRank(graph: Graph[(String, String), Long], iterations: Int = 30, d: Double = 0.85): VertexRDD[Double] = {
    val outWeight = graph.aggregateMessages[Double](c => c.sendToSrc(c.attr.toDouble), _ + _)
    // chaque arete porte la part du poids sortant de sa source ; chaque sommet demarre a 1.0
    var g: Graph[Double, Double] = graph
      .outerJoinVertices(outWeight)((_, _, w) => w.getOrElse(0.0))
      .mapTriplets(t => if (t.srcAttr > 0) t.attr.toDouble / t.srcAttr else 0.0)
      .mapVertices((_, _) => 1.0)
    for (_ <- 1 to iterations) {
      val incoming = g.aggregateMessages[Double](c => c.sendToDst(c.srcAttr * c.attr), _ + _)
      g = g.outerJoinVertices(incoming)((_, _, s) => (1 - d) + d * s.getOrElse(0.0)).cache()
    }
    g.vertices
  }

  def main(args: Array[String]): Unit = {
    val dir = if (args.nonEmpty) args(0) else "reports/graph_exports"

    val spark = SparkSession.builder
      .appName("pedali-graphx")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[4]"))
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // GRAPH_FORMAT=csv : lit <dir>/csv/*.csv (test local sous Windows, ou le Parquet Hadoop est inutilisable) ;
    // par defaut : Parquet (cluster, MinIO).
    val useCsv = sys.env.get("GRAPH_FORMAT").contains("csv")
    def read(name: String) =
      if (useCsv) spark.read.option("header", "true").option("inferSchema", "true").csv(s"$dir/csv/$name.csv")
      else spark.read.parquet(s"$dir/$name")

    val vertices = read("sommets").select("id", "name", "city")
      .as[(String, String, String)].rdd
      .map { case (id, name, city) => (vertexId(id), (name, city)) }

    val edges = read("aretes").select("src", "dst", "trajets")
      .selectExpr("src", "dst", "cast(trajets as long) as trajets")
      .as[(String, String, Long)].rdd
      .map { case (src, dst, n) => Edge(vertexId(src), vertexId(dst), n) }

    val graph = Graph(vertices, edges).cache()
    println(s"Graphe : ${graph.numVertices} sommets, ${graph.numEdges} aretes")

    // --- Degres
    println("\n== Degres (aretes entrantes / sortantes) ==")
    val degrees = graph.inDegrees.join(graph.outDegrees).join(graph.vertices)
      .map { case (id, ((in, out), (name, city))) => (stationId(id), name, city, in, out) }
      .sortBy(_._1)
    degrees.collect().foreach(println)

    // --- PageRank. Non pondere : le reseau est regulier (3 voisins entrants/sortants partout), donc
    // tous les rangs valent 1.0 -> peu informatif. Le PageRank pondere par les trajets distingue les stations.
    println("\n== PageRank non pondere (GraphX, tol = 0.0001) ==")
    graph.pageRank(0.0001).vertices.join(graph.vertices).sortBy(_._1).collect().foreach {
      case (id, (rank, (name, _))) => println(f"${stationId(id)} $name%-16s $rank%.4f")
    }
    println("\n== PageRank pondere par le nombre de trajets (30 iterations) ==")
    val ranks = weightedPageRank(graph).join(graph.vertices)
      .map { case (id, (rank, (name, city))) => (stationId(id), name, city, rank) }
      .sortBy(-_._4)
    ranks.collect().foreach { case (id, name, city, r) => println(f"$id $name%-16s $city%-11s $r%.4f") }

    // --- Composantes connexes
    println("\n== Composantes connexes ==")
    val components = graph.connectedComponents().vertices.join(graph.vertices)
      .map { case (id, (cc, (name, city))) => (cc, stationId(id), city) }
    components.groupBy(_._1).sortByKey().collect().foreach { case (cc, members) =>
      println(s"composante $cc : ${members.map(_._2).toSeq.sorted.mkString(", ")}")
    }

    // --- Plus courts chemins (en nombre de sauts) vers deux stations de reference
    val landmarks = Seq(vertexId("S01"), vertexId("S10"))
    println(s"\n== Plus courts chemins vers ${landmarks.map(stationId).mkString(", ")} (nb de sauts) ==")
    ShortestPaths.run(graph, landmarks).vertices.sortByKey().collect().foreach { case (id, spMap) =>
      println(s"${stationId(id)} -> " + landmarks.map(l => s"${stationId(l)}=${spMap.getOrElse(l, -1)}").mkString(", "))
    }

    // --- Export Parquet (comparaison avec GraphFrames)
    val pagerankDf = ranks.map { case (id, name, city, r) => (id, name, city, r) }.toDF("id", "name", "city", "pagerank")
    scala.util.Try {
      if (useCsv) {
        pagerankDf.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$dir/csv/graphx_pagerank")
        println(s"\nExport CSV : $dir/csv/graphx_pagerank")
      } else {
        pagerankDf.coalesce(1).write.mode("overwrite").parquet(s"$dir/graphx_pagerank")
        println(s"\nExport Parquet : $dir/graphx_pagerank")
      }
    }.recover { case e: Throwable =>
      // Typique sous Windows sans winutils : l'ecriture Hadoop locale est impossible.
      println(s"\nExport ignore (${e.getClass.getSimpleName}) : relancer sur Linux / cluster")
    }

    spark.stop()
  }
}
