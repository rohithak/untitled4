package commonFunctions

import org.apache.spark.sql.SparkSession
//Common place to create spark session - will be useful to add config which will be applicable to all the object using this spark session
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
