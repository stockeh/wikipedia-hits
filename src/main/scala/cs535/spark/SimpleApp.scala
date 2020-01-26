package cs535.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Stock - HITS Wiki Application").getOrCreate()

    import spark.implicits._

    val keyTopic = args(0).toLowerCase()
    val linksInputPath = args(1)
    val output = args(2)

    // RDD(K: Long, T: String)
    val titles = spark.read.textFile(linksInputPath + "/titles-sorted.txt")
      .rdd.zipWithIndex()
      .map { case (v, i) => (i + 1, v.toLowerCase()) }

    val root = titles.filter(line => line._2.contains(keyTopic))

    val links = spark.read.textFile(linksInputPath + "/links-simple-sorted.txt")
      .rdd.map(_.split(":")).map(v => (v(0).toLong, stringToLongArray(v(1))))

    // RDD(K: Long, (T: String, V: Array[Long]))
    val original = root.join(links).map({ case (k, (t, v)) => (k, v) })

    // RDD(K: Long, V: Long)
    val flatLinks = links.flatMapValues(v => v)

    // RDD(V: Long, K: Long)
    val flatLinksInverted = flatLinks.map(_.swap)

    val linksToPageInRoot = root.join(flatLinksInverted).map({ case (k, (t, v)) => (v, k) })
      .join(links).map({ case (k, (z, v)) => (k, v) })

    val linkedToByRoot = root.join(flatLinks).map({ case (k, (t, v)) => (v, k) })
      .join(links).map({ case (k, (z, v)) => (k, v) })

    val base = original.union(linksToPageInRoot).union(linkedToByRoot).map(tup =>
      (tup._1, tup)).reduceByKey({ case (k, v) => k }).map(_._2)

    val incomingLinks = base.flatMapValues(v => v)

    val outgoingLinks = incomingLinks.map(_.swap)
    /*
    println("--- BASE ---")
    base.foreach(v => println(v._1, v._2.toList))

    println("--- INCOMING ( from, to )---")
    incomingLinks.foreach(println(_))

    println("--- OUTGOING ( to, from )---")
    outgoingLinks.foreach(println(_))

    println("--- CALCULATE ---")
    */
    var auth = base.map(v => (v._1, 1.0))
    var hubs = base.map(v => (v._1, 1.0))

    var totalAuth = 0.0;
    var totalHubs = 0.0;
    var prevAuth = 1.0;
    var prevHubs = 1.0;

    val epsilon = 0.001

    while ((prevAuth - totalAuth).abs > epsilon && (prevHubs - totalHubs).abs > epsilon) {

      // authority

      prevAuth = totalAuth;
      val calAuths = incomingLinks.join(hubs).map({ case (from, (to, hub)) => (to, hub) }).reduceByKey(_ + _)

      totalAuth = calAuths.values.sum()
      auth = calAuths.map({ case (k, v) => (k, v / totalAuth) }).rightOuterJoin(auth).map({ case (k, (v1, v2)) => (k, v1.getOrElse(0.0)) })

      // hubs
      prevHubs = totalHubs
      val calcHubs = outgoingLinks.join(auth).map({ case (to, (from, hub)) => (from, hub) }).reduceByKey(_ + _)

      totalHubs = calcHubs.values.sum()
      hubs = calcHubs.map({ case (k, v) => (k, v / totalHubs) }).rightOuterJoin(hubs).map({ case (k, (v1, v2)) => (k, v1.getOrElse(0.0)) })
      /*
      println("--- AUTHS ---")
      calAuths.foreach(println(_))
      println("auth total: " + totalAuth)
      auth.foreach(println(_))

      println("--- HUBS ---")
      calcHubs.foreach(println(_))
      println("hubs total: " + totalHubs)
      hubs.foreach(println(_))
			*/
    }
    val topAuth = spark.sparkContext
      .parallelize(auth.takeOrdered(50)(Ordering[Double].on(-_._2)))
      .join(titles).sortBy(-_._2._1)
      .map { case (k, v) => (v._2, (k, v._1)) }

    val topHubs = spark.sparkContext
      .parallelize(hubs.takeOrdered(50)(Ordering[Double].on(-_._2)))
      .join(titles).sortBy(-_._2._1)
      .map { case (k, v) => (v._2, (k, v._1)) }
    /*
    println("--- FINISHED HUBS ---")
    topHubs.foreach(println(_))

    println("--- FINISHED AUTH ---")
    topAuth.foreach(println(_))
		*/
    topAuth.coalesce(1).saveAsTextFile(output + "-auth")
    topHubs.coalesce(1).saveAsTextFile(output + "-hubs")
  }

  /**
   * Convert the String "1 2 3 4" into an array
   *
   * return a populated array of Longs, or empty array otherwise
   */
  def stringToLongArray(value: String): Array[Long] = {
    val temp = value.trim
    if (temp.nonEmpty) {
      return temp.split("\\s+").map(_.toLong)
    } else {
      return Array.empty[Long]
    }
  }

}
