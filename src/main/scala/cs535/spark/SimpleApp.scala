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

    val titles = spark.read.textFile(linksInputPath + "/test-titles-sorted.txt")
      .rdd.zipWithIndex()
      .map { case (v, i) => (i + 1, v.toLowerCase()) }

    // RDD(Long, String)
    val topics = titles.filter(line => line._2.contains(keyTopic))

    // RDD(Long, Array[Long])
    val links = spark.read.textFile(linksInputPath + "/test-links-simple-sorted.txt")
      .rdd.map(_.split(":")).map(v => (v(0).toLong, stringToLongArray(v(1))))

    // iterate through <links> to get row R <- <k, v> if k in titles add the row to S if not present, then
    // iterate through v and if any values in titles add the row to S if not present.
    val keys = topics.keys.collect()
    val sourceLinks = links.filter(check(_, keys))

    // links with source having title name
    //    val sourceLinks = links.join(topics).map(t => (t._1, t._2._1))
    // links with sink having title name

    val auth = topics.map(v => (v._1, 1.0))
    val hubs = topics.map(v => (v._1, 1.0))

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

    sourceLinks.coalesce(1).mapValues(_.toList).saveAsTextFile(output + "-links")
    topics.coalesce(1).saveAsTextFile(output)
  }

  /**
   * Build the base set to include the root set as well as any page that either links to
   * a page in the root set, or is linked to by a page in the root set.
   *
   * return true if value is contained in RDD, false otherwise
   */
  def check(row: (Long, Array[Long]), topics: Array[Long]): Boolean = {
    if (topics.contains(row._1)) {
      return true
    } else {
      if (!row._2.isEmpty) {
        for (i <- 0 until row._2.length - 1) {
          if (topics.contains(row._2(i))) {
            return true
          }
        }
      }
      return false
    }
  }

  /**
   * Convert the String "1 2 3 4" into an array
   *
   * return a populated array of Longs, or empty array otherwise
   */
  def stringToLongArray(value: String): Array[Long] = {
    println(value)
    val temp = value.trim
    println(temp)
    if (temp.nonEmpty) {
      println("not")
      return temp.split("\\s+").map(_.toLong)
    } else {
      println("is")
      return Array.empty[Long]
    }
  }

}
