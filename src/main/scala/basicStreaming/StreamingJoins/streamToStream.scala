package basicStreaming.StreamingJoins

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import commonFunctions.createSparkSession

object streamToStream {

  val sparksess = createSparkSession.createSparkSess("streamToStream", "local[2]")

  sparksess.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
  }
}