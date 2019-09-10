package org.sparkcourse.SparkStreaming_userbehavior

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.sparkcourse.SparkStreaming_userbehavior.util.KafkaProperties

import scala.util.Random

object KafkaEventProducer {

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val sites = Array(
    "www.baidu.com", "www.google.com",
    "www.bing.com")

  private val browser = Array(
    "chrome", "edge",
    "firefox", "360")

  private val random = new Random()

  val randomSeed = 10

  private var pointer = -1

  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  def click() : Double = {
    random.nextInt(randomSeed)
  }

  def getSite() : String = {
    val num = random.nextInt(randomSeed)
    return sites(num % sites.length)
  }

  def getDuration() : Double = {
    (random.nextInt(10) % 100) // seconds
  }

  def getBrowser() : String = {
    val num = random.nextInt(randomSeed)
    return browser(num % browser.length)
  }

  def main(args: Array[String]): Unit = {
    val topic = KafkaProperties.KAFKA_USER_TOPIC
    val brokers = KafkaProperties.KAFKA_ADDR
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while(true) {
      // prepare event data
      val event = new JSONObject()
      event.put("uid", getUserID)
      event.put("event_time", System.currentTimeMillis.toString)
      event.put("os_type", "Android")
      event.put("click_count", click)
      event.put("site", getSite)
      event.put("session_duration", getDuration)
      event.put("browser", getBrowser)

      // produce event message
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(200)
    }
  }
}
