package commonFunctions

import org.apache.spark.sql.SparkSession

object createSparkSession {
  def createSparkSess(appName: String, masterConf: String): SparkSession = {
    val sparkSess = SparkSession
      .builder
      .master(masterConf)
      .config("spark.sql.warehouse.dir", "D:/Temp/sparkwarehouse")
      .appName(appName)
      .getOrCreate()

    return sparkSess
  }
}