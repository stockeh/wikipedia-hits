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
    val titles = spark.read.textFile(linksInputPath + "/titles-sorted.txt")
      .rdd.zipWithIndex()
      .map { case (v, i) => (i + 1, v.toLowerCase()) }

    // RDD(K: Long, T: String)
    val root = titles.filter(line => line._2.contains(keyTopic))

    // RDD(K: Long, V: Array[Long])
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

    val totalAuth = 0
    val totalHubs = 0
    val prevAuth = 1
    val prevHubs = 1

    val epsilon = 0.001

    //    while ((prevAuth - totalAuth).abs > epsilon && (prevHubs - totalHubs).abs > epsilon) {
    //
    //      // iteratively update <auth> and <hubs>
    //
    //    }

    base.coalesce(1).mapValues(_.toList).saveAsTextFile(output + "-links")
    root.coalesce(1).saveAsTextFile(output)
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
