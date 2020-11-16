package basicStreaming.aggregations

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column

object groupByOnNC {

  val sparksess = SparkSession
    .builder()
    .appName("ncReadAggregations")
    .master("local[2]")
    .getOrCreate()

  sparksess.sparkContext.setLogLevel("INFO")

  def groupInputData() {
    val inputDF = sparksess
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9872)
      .load()

    val aggregationDF = inputDF.select(col("value")).groupBy(col("value")).count

    val query = aggregationDF
      .writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    query.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    groupInputData()
  }
}