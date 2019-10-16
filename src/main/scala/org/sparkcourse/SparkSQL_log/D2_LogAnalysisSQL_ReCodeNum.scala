package org.sparkcourse.SparkSQL_log

import org.apache.spark.sql.SparkSession

object D2_LogAnalysisSQL_ReCodeNum {
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

    // 统计每种返回码的数量.
        val responseCodeToCount = spark.sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
          .map(row => (row.getInt(0), row.getLong(1)))
          .collect()
        responseCodeToCount.foreach(print(_))
  }
}
