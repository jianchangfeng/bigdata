package org.sparkcourse.SparkStreaming_userbehavior

import com.alibaba.fastjson.JSON
import org.apache.spark._
import org.apache.spark.streaming._

object D3_SiteAnalytics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("UserClick")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("data/checkpoint")
    // 输入
    val events = ssc.socketTextStream("localhost", 9998)
      .flatMap(line => {
        val data = JSON.parseObject(line)
        Some(data)
      })
    // 分析处理
    val results = events.map(x => {
      (x.getString("site"), x.getLong("click_count"))
    }).reduceByKeyAndWindow(_+_, _-_, Seconds(10), Seconds(5))
      .foreachRDD(rdd => {
        rdd.foreach(x => println("results %s".format(x)))
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
