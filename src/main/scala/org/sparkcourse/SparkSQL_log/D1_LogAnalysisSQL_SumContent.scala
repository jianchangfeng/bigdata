package org.sparkcourse.SparkSQL_log

import org.apache.spark.sql.{Row, SparkSession}

object D1_LogAnalysisSQL_SumContent {
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

     //统计分析内容大小-全部内容大小，日志条数，最小内容大小，最大内容大小
    val contentSizeStats: Row = spark.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs").first()
    val sum = contentSizeStats.getLong(0)
    val count = contentSizeStats.getLong(1)
    val min = contentSizeStats.getLong(2)
    val max = contentSizeStats.getLong(3)
    println("sum %s, count %s, min %s, max %s".format(sum, count, min, max))
    println("avg %s", sum / count)
    spark.close()
  }
}
