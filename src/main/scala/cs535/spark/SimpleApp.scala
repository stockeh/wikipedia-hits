package cs535.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Hyperlink-Induced Topic Search (HITS) over Wikipedia Articles using Apache Spark
 *
 */
object SimpleApp {

  val DEBUG = false;

  /**
   * Driver method to run HITS algorithm.
   *
   * args keyTopic, epsilon, linksInputPath, output
   *
   */
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Stock - HITS Wiki Application").getOrCreate()

    import spark.implicits._

    val keyTopic = args(0).toLowerCase()
    val epsilon = args(1).toDouble
    val linksInputPath = args(2)
    val output = args(3)

    val titles = spark.read.textFile(linksInputPath + "/titles-sorted.txt")
      .rdd.zipWithIndex()
      .map { case (v, i) => (i + 1, v.toLowerCase()) }

    val links = spark.read.textFile(linksInputPath + "/links-simple-sorted.txt")
      .rdd.map(_.split(":")).map(v => (v(0).toLong, stringToLongArray(v(1))))

    val base = createBaseSet(titles, links, keyTopic)

    calculateScores(spark, titles, base, epsilon, output)

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

  /**
   * Create the base set by first creating the root set and then going
   * through incoming and outgoing links
   *
   * return an RDD of all the links in the base set
   */
  def createBaseSet(
    titles: RDD[(Long, String)],
    links: RDD[(Long, Array[Long])], keyTopic: String): RDD[(Long, Array[Long])] = {

    val root = titles.filter(line => line._2.contains(keyTopic))

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

    return original.union(linksToPageInRoot).union(linkedToByRoot).map(tup =>
      (tup._1, tup)).reduceByKey({ case (k, v) => k }).map(_._2)
  }

  /**
   * Calculate the hubs and authority scores based on the base set.
   *
   * Save the results to HDFS
   *
   */
  def calculateScores(
    spark: SparkSession, titles: RDD[(Long, String)],
    base: RDD[(Long, Array[Long])], epsilon: Double, output: String) = {

    var auth = base.map(v => (v._1, 1.0))
    var hubs = base.map(v => (v._1, 1.0))

    var totalAuth = 0.0;
    var totalHubs = 0.0;
    var prevAuth = 1.0;
    var prevHubs = 1.0;

    val incomingLinks = base.flatMapValues(v => v)
    val outgoingLinks = incomingLinks.map(_.swap)

    if (DEBUG) {
      println("--- BASE ---")
      base.foreach(v => println(v._1, v._2.toList))

      println("--- INCOMING ( from, to )---")
      incomingLinks.foreach(println(_))

      println("--- OUTGOING ( to, from )---")
      outgoingLinks.foreach(println(_))

      println("--- CALCULATE ---")
    }

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

      if (DEBUG) {
        println("--- AUTHS ---")
        calAuths.foreach(println(_))
        println("auth total: " + totalAuth)
        auth.foreach(println(_))

        println("--- HUBS ---")
        calcHubs.foreach(println(_))
        println("hubs total: " + totalHubs)
        hubs.foreach(println(_))
      }
    }
    saveResults(spark, titles, auth, hubs, output)
  }

  /**
   * Sort and save results to HDFS.
   *
   */
  def saveResults(spark: SparkSession, titles: RDD[(Long, String)],
    auth: RDD[(Long, Double)], hubs: RDD[(Long, Double)], output: String) = {

    val topAuth = spark.sparkContext
      .parallelize(auth.takeOrdered(50)(Ordering[Double].on(-_._2)))
      .join(titles).sortBy(-_._2._1)
      .map { case (k, v) => (v._2, (k, v._1)) }

    val topHubs = spark.sparkContext
      .parallelize(hubs.takeOrdered(50)(Ordering[Double].on(-_._2)))
      .join(titles).sortBy(-_._2._1)
      .map { case (k, v) => (v._2, (k, v._1)) }

    if (DEBUG) {
      println("--- FINISHED HUBS ---")
      topHubs.foreach(println(_))

      println("--- FINISHED AUTH ---")
      topAuth.foreach(println(_))
    }

    topAuth.coalesce(1).saveAsTextFile(output + "-auth")
    topHubs.coalesce(1).saveAsTextFile(output + "-hubs")
  }

}
