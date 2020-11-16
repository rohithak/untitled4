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

object ncReadAggregations {

  val sparksess = SparkSession
    .builder()
    .appName("ncReadAggregations")
    .master("local[2]")
    .getOrCreate()

  sparksess.sparkContext.setLogLevel("INFO")

  def countLinesFromSocket() {
    val inputDF = sparksess
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9876)
      .load()

    val aggDf = inputDF.selectExpr("count(*) as countOfLines")

    val query = aggDf
      .writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    query.awaitTermination()
  }

  //passing function as argument whcih takes Column type and returns COlumn type
  def numericalAggregation(agg: Column => Column) {

    val inputDF = sparksess
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9876)
      .load()

    // aggregate inputDF
    val valAsNum = inputDF.select(col("value").cast("integer").as("number"))
    val aggregationDF = valAsNum.select(agg(col("number")).as("Agg_Func_OP"))

    val query = aggregationDF
      .writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //countLinesFromSocket()
    //Any Agg function can be passed as argument
    numericalAggregation(sum)

  }

}


/*
 Console output won't support checkpoint
 
 "id" : "f81306b6-769b-4f4a-a63e-3a183445b60a",
 Returns the unique id of this query that persists across restarts from checkpoint data. That is, this id is generated when a query is started for the first time, and will be the same every time it is restarted from checkpoint data 
 
 "runId" : "b47126e6-07d5-438a-8022-ce99d4efab8c"
 Returns the unique id of this run of the query. That is, every start/restart of a query will generated a unique runId. Therefore, every time a query is restarted from checkpoint, it will have the same id but different runIds.
 
 "batchId" : 2,
 shows current batch of Spark streaming application
 
 
 startOffset and endOffset would signify the reading points of source
 	"startOffset" : 2,
	"endOffset" : 12,
  "numInputRows" : 10,

  "id" : "f81306b6-769b-4f4a-a63e-3a183445b60a",
  "runId" : "b47126e6-07d5-438a-8022-ce99d4efab8c",
  "name" : null,
  "timestamp" : "2020-11-16T05:23:08.000Z",
  "batchId" : 2,
  "numInputRows" : 10,
  "inputRowsPerSecond" : 5.002501250625312,
  "processedRowsPerSecond" : 6.361323155216285,
  "durationMs" : {
    "addBatch" : 973,
    "getBatch" : 0,
    "latestOffset" : 0,
    "queryPlanning" : 26,
    "triggerExecution" : 1572,
    "walCommit" : 264
  },
  "stateOperators" : [ {
    "numRowsTotal" : 1,
    "numRowsUpdated" : 1,
    "memoryUsedBytes" : 664,
    "customMetrics" : {
      "loadedMapCacheHitCount" : 4,
      "loadedMapCacheMissCount" : 0,
      "stateOnCurrentVersionSizeBytes" : 248
    }
  } ],
  "sources" : [ {
    "description" : "TextSocketV2[host: localhost, port: 9876]",
    "startOffset" : 2,
    "endOffset" : 12,
    "numInputRows" : 10,
    "inputRowsPerSecond" : 5.002501250625312,
    "processedRowsPerSecond" : 6.361323155216285
  } ],
  "sink" : {
    "description" : "org.apache.spark.sql.execution.streaming.ConsoleTable$@2170b084",
    "numOutputRows" : 1
  }
}



*/
