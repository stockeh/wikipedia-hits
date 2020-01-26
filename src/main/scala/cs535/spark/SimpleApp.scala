package cs535.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    import spark.implicits._

    val keyTopic = args(0).toLowerCase()
    val linksInputPath = args(1)
    val output = args(2)

    // RDD(K: Long, T: String)
    val titles = spark.read.textFile(linksInputPath + "/test-titles-sorted-2.txt")
      .rdd.zipWithIndex()
      .map { case (v, i) => (i + 1, v.toLowerCase()) }

    val root = titles.filter(line => line._2.contains(keyTopic))

    val links = spark.read.textFile(linksInputPath + "/test-links-simple-sorted-2.txt")
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

    println("--- BASE ---")
    base.foreach(v => println(v._1, v._2.toList))

    val incomingLinks = base.flatMapValues(v => v).map(_.swap).groupByKey().flatMapValues(v => v).map(_.swap)
    
    println("--- INCOMING ---")
    incomingLinks.foreach(println(_))

    println("--- CALCULATE ---")

    var auth = base.map(v => (v._1, 1.0))
    var hubs = base.map(v => (v._1, 1.0))

    val epsilon = 0.001

    for (i <- 0 to 0) {

      // authority

      val auths = incomingLinks.join(hubs).map({ case (from, (to, hub)) => (to, hub) }).reduceByKey(_ + _)
      auths.foreach(println(_))

      var totalAuth = auths.values.sum()
      println("auth total: " + totalAuth)
      auth = auths.map({ case (k, v) => (k, v / totalAuth) }).rightOuterJoin(auth).map({ case (k, (v1, v2)) => (k, v1.getOrElse(0.0)) })
      auth.foreach(println(_))

      // hubs
      
    }

    //    while ((prevAuth - totalAuth).abs > epsilon && (prevHubs - totalHubs).abs > epsilon) {
    //
    //      // iteratively update <auth> and <hubs>
    //
    //    }

    //    base.coalesce(1).mapValues(_.toList).saveAsTextFile(output + "-links")
    //    root.coalesce(1).saveAsTextFile(output)
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
