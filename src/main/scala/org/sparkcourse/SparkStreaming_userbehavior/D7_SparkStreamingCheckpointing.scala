package org.sparkcourse.SparkStreaming_userbehavior
import com.alibaba.fastjson.JSON
import org.apache.spark._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

// Why checkpointing: http://spark.apache.org/docs/latest/streaming-kafka-0-8-integration.html
object D7_SparkStreamingCheckpointing {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local[*]"
    val checkpoint = if (args.length > 1) args(1).toString else "_checkpoint"
    val topic = if (args.length > 2) args(2).toString else "test"
    val broker = if (args.length > 3) args(3).toString else "node001:9092"
    val conf = new SparkConf().setMaster(master).setAppName("UserClick")
    val checkpointDir = checkpoint
    // 创建StreamingContext
    def createSSC(): StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(1))
      socketTest(ssc)
      // kafkaTest(ssc, topic, broker)
      // 设置检查点
      ssc.checkpoint(checkpointDir)
      ssc
    }
    // 如果重启，可以从检查点恢复
    val ssc = StreamingContext.getOrCreate(checkpointDir, createSSC)

    ssc.start()
    ssc.awaitTermination()
  }

  def socketTest(ssc: StreamingContext): Unit = {
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
  }

  def kafkaTest(ssc: StreamingContext, topicparam: String, brokerparam: String): Unit = {
    val topic = topicparam
    val broker = brokerparam

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
  }
}
