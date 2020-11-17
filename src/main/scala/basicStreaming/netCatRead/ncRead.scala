package basicStreaming.netCatRead

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import commonFunctions.createSparkSession

object ncRead {

  val sparkSession = createSparkSession.createSparkSess("netCat and file directory read", "local[2]")

  def readSocket() {
    val inputDF = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1234)
      .load()

    val lessCharacter: DataFrame = inputDF.filter(length(col("value")).<=(5))

    lessCharacter.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    readSocket()
  }
}