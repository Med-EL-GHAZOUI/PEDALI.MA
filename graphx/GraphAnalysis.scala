// Partie 6 - Analyse du reseau de stations avec GraphX (Scala, API RDD).
//
// Entree : exports Parquet de graphframes/graph_analysis.py (s3a://lake/gold/graph_export/{sommets,aretes}),
// donc GraphX et GraphFrames travaillent sur exactement le meme graphe. Sommets = stations, aretes = trajets
// termines agreges par couple (attribut = nombre de trajets).
// Calculs : degres, PageRank (non pondere et pondere), connectedComponents, ShortestPaths vers deux gares de reference.
// Sortie : s3a://lake/gold/graph_analysis/ (mode append, partitionne par city et result_type).
//
// Execution (Spark 3.5.3, Scala 2.12) : voir graphx/README.md et scripts/run_graphx.sh.
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession

object GraphAnalysis {

  // PageRank pondere par le nombre de trajets (graph.pageRank de GraphX ignore l'attribut des aretes).
  // Meme convention que GraphX : rang non normalise, la somme vaut le nombre de sommets.
  private def weightedPageRank(graph: Graph[(String, String, String, String), Long], iterations: Int = 30, d: Double = 0.85): VertexRDD[Double] = {
    val outWeight = graph.aggregateMessages[Double](c => c.sendToSrc(c.attr.toDouble), _ + _)
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

  private def timed[T](label: String)(body: => T): T = {
    val t = System.nanoTime
    val r = body
    println(f"[temps] $label%-34s ${(System.nanoTime - t) / 1e9}%.1f s")
    r
  }

  def main(args: Array[String]): Unit = {
    val lake = if (args.nonEmpty) args(0) else "s3a://lake"
    val spark = SparkSession.builder.appName("pedali-graphx").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val vdf = spark.read.parquet(s"$lake/gold/graph_export/sommets").select("id", "name", "city", "zone_type")
    val edf = spark.read.parquet(s"$lake/gold/graph_export/aretes").select("src", "dst", "trajets")

    // Identifiant numerique unique (Long) par station : rang dans l'ordre alphabetique des identifiants.
    val idMap = vdf.select("id").as[String].rdd.sortBy(identity).zipWithIndex().collectAsMap()
    val bId = sc.broadcast(idMap)
    val names = idMap.map(_.swap)                               // Long -> "S-CAS-005"

    val vertices = vdf.as[(String, String, String, String)].rdd
      .map { case (id, name, city, zone) => (bId.value(id), (id, name, city, zone)) }
    val edges = edf.selectExpr("src", "dst", "cast(trajets as long) as trajets").as[(String, String, Long)].rdd
      .map { case (s, d, n) => Edge(bId.value(s), bId.value(d), n) }
    val graph = Graph(vertices, edges).cache()
    println(s"Graphe GraphX : ${graph.numVertices} sommets, ${graph.numEdges} aretes")

    // --- Degres
    val degrees = timed("degres") {
      graph.inDegrees.join(graph.outDegrees).join(graph.vertices).map {
        case (vid, ((in, out), (sid, name, city, zone))) => (sid, city, in, out)
      }.collect().sortBy(_._1)
    }
    println("\n== Degres (voisins entrants / sortants) : 5 premieres stations ==")
    degrees.take(5).foreach(println)

    // --- PageRank
    val pr = timed("PageRank non pondere (tol 1e-4)") { val r = graph.pageRank(0.0001).vertices.cache(); r.count(); r }
    val wpr = timed("PageRank pondere (30 iterations)") { val r = weightedPageRank(graph).cache(); r.count(); r }
    val ranks = wpr.join(pr).join(graph.vertices).map {
      case (vid, ((w, u), (sid, name, city, zone))) => (sid, name, city, zone, w, u)
    }.collect().sortBy(-_._5)
    println("\n== PageRank pondere par le nombre de trajets : 10 premieres stations ==")
    ranks.take(10).foreach { case (sid, name, city, zone, w, u) => println(f"$sid%-10s $city%-11s $zone%-16s pondere=$w%.3f non pondere=$u%.3f") }

    // --- Composantes connexes
    val cc = timed("connectedComponents") { val r = graph.connectedComponents().vertices.cache(); r.count(); r }
    val comps = cc.join(graph.vertices).map { case (vid, (c, (sid, _, city, _))) => (c, sid, city) }.collect()
    println("\n== Composantes connexes (sous-reseaux isoles) ==")
    comps.groupBy(_._1).toSeq.sortBy(-_._2.length).foreach { case (c, ms) =>
      val cities = ms.map(_._3).distinct.mkString(", ")
      println(s"composante $c : ${ms.length} stations (${cities})")
    }

    // --- Plus courts chemins (nombre de sauts) vers la gare la plus connectee de Casablanca et de Marrakech
    val deg = degrees.map(d => (d._1, d._3 + d._4)).toMap
    val gares = graph.vertices.map { case (vid, (sid, _, city, zone)) => (sid, city, zone) }.collect()
      .filter(g => g._3 == "gare" && Seq("Casablanca", "Marrakech").contains(g._2))
    val landmarks = Seq("Casablanca", "Marrakech").map(c => gares.filter(_._2 == c).maxBy(g => deg.getOrElse(g._1, 0))._1)
    val lmIds = landmarks.map(bId.value(_))
    println(s"\n== Plus courts chemins (sauts) vers ${landmarks.mkString(", ")} ==")
    val sp = timed("ShortestPaths") { ShortestPaths.run(graph, lmIds).vertices.collect().toMap }
    val reach = lmIds.map(l => l -> sp.values.count(_.contains(l))).toMap
    lmIds.zip(landmarks).foreach { case (l, name) =>
      val hops = sp.values.flatMap(_.get(l)).toSeq
      println(f"$name : ${reach(l)} stations atteignent cette gare, distance moyenne ${if (hops.isEmpty) 0.0 else hops.sum.toDouble / hops.length}%.2f sauts, maximum ${if (hops.isEmpty) 0 else hops.max}")
    }

    // --- Ecriture dans gold/graph_analysis (schema commun : result_type, city, entity, value, detail)
    val cityOf = graph.vertices.map { case (vid, (sid, _, city, _)) => (sid, city) }.collect().toMap
    val compSize = comps.groupBy(_._1).map { case (c, ms) => (c, ms.length) }
    val rows =
      ranks.map { case (sid, _, city, zone, w, u) => ("pagerank_graphx", city, sid, w, s"""{"pagerank_non_pondere":$u,"zone_type":"$zone"}""") }.toSeq ++
      degrees.map { case (sid, city, in, out) => ("degres_graphx", city, sid, (in + out).toDouble, s"""{"voisins_entrants":$in,"voisins_sortants":$out}""") }.toSeq ++
      comps.map { case (c, sid, city) => ("composantes_graphx", city, sid, c.toDouble, s"""{"taille":${compSize(c)}}""") }.toSeq ++
      sp.toSeq.flatMap { case (vid, m) =>
        val sid = names(vid)
        m.toSeq.map { case (l, hops) => ("plus_courts_chemins_graphx", cityOf(sid), s"$sid->${names(l)}", hops.toDouble, "{}") }
      }
    val out = rows.toDF("result_type", "city", "entity", "value", "detail")
    out.write.mode("append").partitionBy("city", "result_type").parquet(s"$lake/gold/graph_analysis")
    println(s"\nExport Parquet GraphX : $lake/gold/graph_analysis (${rows.length} lignes ajoutees)")
    spark.stop()
  }
}
