package org.sparkcourse.SparkCore_movie
import org.apache.spark.{SparkConf, SparkContext}

object Top3UserSawmoive {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val conf = new SparkConf().setMaster(master).setAppName("TopKMovie").set("spark.io.compression.codec", "snappy")
    val sc = new SparkContext(conf)

    // 输入
    val ratingsRdd = sc.textFile("data/ml-1m/ratings.dat")

    // 数据抽取
    val ratings = ratingsRdd.map(_.split("::"))
      .map { x => (x(0), x(1), x(2)) // userid, movieid, rating
    }.cache
    val topKmostPerson = ratings.map{ x =>
      (x._1, 1)
    }.reduceByKey(_ + _).
      map(x => (x._2, x._1)).
      sortByKey(false).
      take(3).
      foreach(println)
  }

}
