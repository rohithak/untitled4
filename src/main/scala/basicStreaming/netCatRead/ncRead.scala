package basicStreaming.netCatRead

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object ncRead {

  val sparkSession = SparkSession
    .builder()
    .appName("netCat and file directory read")
    .master("local[2]")
    .getOrCreate()

  def readSocket() {
    val inputDF = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lessCharacter: DataFrame = inputDF.filter(length(col("value")).<=(5))

    inputDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    readSocket()
  }
}