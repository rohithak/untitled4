package basicStreaming.aggregations

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

object groupByOnNC {

  val sparksess = SparkSession
    .builder()
    .appName("groupByOnNC")
    .master("local[2]")
    .getOrCreate()

  sparksess.sparkContext.setLogLevel("ERROR")

  def groupNCContent(): Unit = {
    val content: DataFrame = sparksess.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurrences of the "name" value
    val contentAgg = content
      .select(col("value"))
      .groupBy(col("value")) // RelationalGroupedDataset - apply aggregate on top of this
      .count()

    contentAgg.writeStream
      .format("console")
      .outputMode("complete") // no water marking
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    groupNCContent()
  }
}