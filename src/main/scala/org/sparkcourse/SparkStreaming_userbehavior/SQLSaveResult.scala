package org.sparkcourse.SparkStreaming_userbehavior

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// Ref: https://www.cnblogs.com/hmy-blog/p/7798840.html 
//1 mysql -hconnectionstring -uroot -pHadoop@xiaoxiang
//2 create database test;
//3 use test;
//4 show tables;
//5 create table result(uid varchar(30), click_count int);
object SQLSaveResult {
  def main(args: Array[String]){
    val master = if (args.length > 0) args(0).toString else "local[*]"
    val hostname = if (args.length > 1) args(1).toString else "localhost"
    val port = if (args.length > 2) args(2).toInt else 9998

    val conf = new SparkConf().setMaster(master).setAppName("UserClick")
    val ssc = new StreamingContext(conf, Seconds(1))

    // 输入
    val events = ssc.socketTextStream(hostname, port)
      .flatMap(line => {
        val data = JSON.parseObject(line)
        Some(data)
      })
    // 分析
    val userClicks = events.map(x => {
      (x.getString("uid"), x.getLong("click_count"))
    }).reduceByKey(_+_)
      .foreachRDD(rdd => {
        // 将RDD结果写入到MySQL
        rdd.foreachPartition(eachPartition => {
          val conn = DBConnectionPool.getConnection();
          eachPartition.foreach(record => {
            val sql = "insert into result(uid, click_count) values('" + record._1 + "'," + record._2 + ")"
            val stmt = conn.createStatement
            stmt.executeUpdate(sql)
          })
          DBConnectionPool.returnConnection(conn)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}