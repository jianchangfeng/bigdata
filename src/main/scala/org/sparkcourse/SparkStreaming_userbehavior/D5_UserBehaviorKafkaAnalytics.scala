package org.sparkcourse.SparkStreaming_userbehavior

import kafka.serializer.StringDecoder
import com.alibaba.fastjson.JSON
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

object D5_UserBehaviorKafkaAnalytics {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local[*]"
    val topic = if (args.length > 1) args(1).toString else "test"
    val broker = if (args.length > 2) args(2).toString else "roc15:9092"
    val conf = new SparkConf().setMaster(master).setAppName("UserClick")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Kafka configurations
    val topics = topic.split("\\,").toSet
    println(s"Topics: ${topics}.")

    val brokers = broker
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )

    // Create a direct stream
    val kafkaStream = KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      println(s"Line ${line}.")
      val data = JSON.parseObject(line._2)
      Some(data)
    })
    
    val userClicks = events.map(x => {
      (x.getString("uid"), x.getLong("click_count"))
    }).reduceByKey(_+_)
      .foreachRDD(rdd => {
        rdd.collect().foreach(x => println("results %s".format(x)))
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
