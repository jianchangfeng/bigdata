package org.sparkcourse.SparkSQL_log

import org.apache.spark.sql.SparkSession

object D3_LogAnalysisSQL_ipAddress {
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

    // 统计哪个IP地址访问服务器超过10次
      val ipAddresses = spark.sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
        .map(row => row.getString(0))
        .collect()
      ipAddresses.foreach(println(_))

  }

}
