package basicStreaming.StreamingJoins

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.JoinType
import commonFunctions.createSparkSession

object staticDFToStream {
  val sparksess = createSparkSession.createSparkSess("staticDFToStream", "local[2]")

  sparksess.sparkContext.setLogLevel("ERROR")

  def loadStaticDF(): (DataFrame, DataFrame, DataFrame) = {

    val guitarPlayers = sparksess.read
      .option("inferSchema", true)
      .json("./src/main/resources/data/guitarPlayers")

    val guitars = sparksess.read
      .option("inferSchema", true)
      .json("./src/main/resources/data/guitars")

    val bands = sparksess.read
      .option("inferSchema", true)
      .json("./src/main/resources/data/bands")

    (guitarPlayers, guitars, bands)
  }
  def joinStaticDFWithStream() {

    val (guitarPlayers, guitars, bands) = loadStaticDF

    val joinCondition = guitarPlayers.col("band") === bands.col("id")
    val bandsSchema = bands.schema
    bandsSchema.printTreeString()

    val bandsDFAsStream = sparksess.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val joinedDF = guitarPlayers.join(bandsDFAsStream, guitarPlayers.col("band") === bandsDFAsStream.col("id"), "right")

    joinedDF.
      writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {

    joinStaticDFWithStream()
  }

}


    /*
      allowed joins:
      - stream IJ static: allowed
      - stream LJ static: allowed
      - static RJ stream: allowed
      
      restricted joins:
      - stream RJ static:  Right outer join with a streaming DataFrame/Dataset on the left and a static DataFrame/DataSet on the right not supported;;
     
     
     */


