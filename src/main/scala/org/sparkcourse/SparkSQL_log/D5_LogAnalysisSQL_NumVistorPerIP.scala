package org.sparkcourse.SparkSQL_log

import org.apache.spark.sql.SparkSession

object D5_LogAnalysisSQL_NumVistorPerIP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Log Analysize")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val accessLogs = spark
      .read
      .textFile("data/weblog/apache.access.log")
      .map(ApacheAccessLog.parseLogLine).toDF()

    accessLogs.createOrReplaceTempView("logs")

    val topEndpoints = spark.sql("SELECT ipAddress, COUNT(*) FROM logs GROUP BY ipAddress")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()

    topEndpoints.foreach(println(_))

  }

}
