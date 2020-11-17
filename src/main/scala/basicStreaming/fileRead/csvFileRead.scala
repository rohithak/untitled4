package basicStreaming.fileRead

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import commonFunctions.createSparkSession

object csvFileRead {

  //spark session is a unified entry point for the spark application which is being built
  //using spark session, users can access spark functionality with lesser number of constructs.
  // once spark session is created, users can create DataFrame, DataSet, RDD, write Spark sql queries..

  // master- this would set the Spark master URL to connect to,
  // such as "local" to run locally,
  // "local[4]" to run locally with 4 cores, or
  // "spark://master:7077" to run on a Spark standalone cluster.
  //for configuring this for an application, it is always better to load it from configuration file.

  val sparkSession = createSparkSession.createSparkSess("CSV file read as stream", "local[2]")

  def readFile() {

    // read stream is the access point for DataStreamReader
    // DataStreamReader  - allowed users to describe  how spark streaming loads dataset from a streaming source
    val inputDF = sparkSession
      .readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("""D:\SparkStreaming\Test\SampleCSV""")
    //Change directory according to env

    //awaitTermination = waits for the termination signal from user or terminates program on reception of exception
    //upon reception of CTRL+C or SIGTERM, streaming context will be stopped.
    //throw the reported error during the execution

    // start method will return a streaming query

    //Trigger - create a batch every 2 seconds (specified time). Spark will query data (input direcotry) every 2 seconds and it will batch the incoming data in time that time window and apply transformation if any and output the result to a output sink
    inputDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      //trigger(Trigger.Once()) // single batch, then terminate
      //trigger(Trigger.Continuous(interval)) // every 2 seconds will query - it is continuous processing of data, long running query, very low latency
      .start()
      .awaitTermination()
  }

  val stocksSchema = StructType(Array(
    StructField("company", StringType),
    StructField("date", DateType),
    StructField("value", DoubleType)))

  def main(args: Array[String]): Unit = {
    readFile()
  }

}


/*

Sample File

AAPL,Jan 1 2000,25.94
AAPL,Feb 1 2000,28.66
AAPL,Mar 1 2000,33.95
AAPL,Apr 1 2000,31.01
AAPL,May 1 2000,21
AAPL,Jun 1 2000,26.19
AAPL,Jul 1 2000,25.41
AAPL,Aug 1 2000,30.47
AAPL,Sep 1 2000,12.88
AAPL,Oct 1 2000,9.78
AAPL,Nov 1 2000,8.25
AAPL,Dec 1 2000,7.44
AAPL,Jan 1 2001,10.81
AAPL,Feb 1 2001,9.12
AAPL,Mar 1 2001,11.03
AAPL,Apr 1 2001,12.74
AAPL,May 1 2001,9.98
AAPL,Jun 1 2001,11.62
AAPL,Jul 1 2001,9.4
AAPL,Aug 1 2001,9.27



*/