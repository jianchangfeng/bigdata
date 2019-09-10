package org.sparkcourse.SparkStreaming_userbehavior

import com.alibaba.fastjson.JSON
import org.apache.spark._
import org.apache.spark.streaming._

object UserBehaviorSocketAnalytics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("UserClick").set("spark.io.compression.codec", "snappy")
    val ssc = new StreamingContext(conf, Seconds(1))

    // 输入
    val events = ssc.socketTextStream("localhost", 9998)
      .flatMap(line => {
        val data = JSON.parseObject(line)
        Some(data)
      })
    // 分析
    val userClicks = events.map(x => {
      (x.getString("uid"), x.getLong("click_count"))
    }).reduceByKey(_+_)
      .foreachRDD(rdd => {
        rdd.foreach(x => println("results %s".format(x)))
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
