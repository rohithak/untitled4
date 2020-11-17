package basicStreaming.StreamingJoins

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column

object streamToStream {
  val sparksess = SparkSession
    .builder()
    .appName("streamToStream")
    .master("local[2]")
    .getOrCreate()

  sparksess.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
  }
}