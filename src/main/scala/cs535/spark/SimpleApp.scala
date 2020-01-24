/* SimpleApp.scala */
package cs535.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    import spark.implicits._

    val linksInputPath = args(0)
    val output = args(1)

    val logData = spark.read.textFile("hdfs://earth:32351/data/PA1/titles-sorted.txt")
    
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    val data = spark.sparkContext.parallelize(Seq(("A", numAs), ("B", numBs)))
    data.coalesce(1).saveAsTextFile(output)
  }
}
