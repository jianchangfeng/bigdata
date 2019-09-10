package org.sparkcourse.SparkSQL_log

import org.apache.spark.sql.SparkSession

object LogAnalysisSQL_TopVisitor {
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
    // 查询访问量最大的访问目的地址
    val topEndpoints = spark.sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    topEndpoints.foreach(println(_))

  }
}
