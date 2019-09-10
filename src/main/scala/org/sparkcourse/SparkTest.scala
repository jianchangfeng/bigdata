package org.sparkcourse

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val num = if (args.length > 1) args(1).toInt else 10
    val conf = new SparkConf().setMaster(master).setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to num).filter(_==1).take(1)
    rdd.foreach(println(_))
  }

}
