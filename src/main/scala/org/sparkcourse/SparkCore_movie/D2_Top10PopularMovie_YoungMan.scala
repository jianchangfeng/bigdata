package org.sparkcourse.SparkCore_movie

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

object D2_Top10PopularMovie_YoungMan {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val conf = new SparkConf().setMaster(master).setAppName("PopularMovie").set("spark.io.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    // 输入
    val usersRdd = sc.textFile("data/ml-1m/users.dat")
    val ratingsRdd = sc.textFile("data/ml-1m/ratings.dat")
    val moviesRdd = sc.textFile("data/ml-1m/movies.dat")

    // 抽取数据和过滤
    val users = usersRdd.map(_.split("::")).map(x => {
      (x(0), x(2)) // userid , age
    }).filter(x => x._2.toInt >= 20 && x._2.toInt <= 30)
      .map(_._1)
      .collect()
    val userSet = HashSet() ++ users
    val broadcastUserSet = sc.broadcast(userSet)

    //join movie and rating
    // RDD[(movieID,userID)]
    val rating_2 = ratingsRdd.map(_.split("::")).map(x => (x(1),x(0)))
    // RDD[(movieID, movieName)]
    val movie = moviesRdd.map(_.split("::")).map(x=> (x(0),x(1)))

    //先获取movieID 对应的movieName
    //RDD[movieID,(UserID,movieName)]
    //(2828,(16,Dudley Do-Right (1999)))
    val movieRating2 = rating_2.join(movie)
    movieRating2.take(1).foreach(println(_))

    val rating_3 = movieRating2.map(x=>{(x._2._1,x._2._2)})
    rating_3.take(1).foreach(println(_))


    // 聚合和排序
    println("年龄段在”20~30” 的年轻人，最喜欢看哪10部电影")
    val topKMovies = rating_3 // userid, movieName
      .filter(x => {
      broadcastUserSet.value.contains(x._1)
    }).map(x => {
      (x._2, 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => {(x._2, x._1)})
      .take(10)
      .foreach(println(_))

//    val topKMovies = ratingsRdd.map(_.split("::"))
//      .map(x => {(x(0), x(1))}) // userid, movieid
//      .filter(x => {
//      broadcastUserSet.value.contains(x._1)
//    }).map(x => {
//      (x._2, 1)
//    }).reduceByKey(_ + _)
//      .map(x => (x._2, x._1))
//      .sortByKey(false)
//      .map(x => {(x._2, x._1)})
//      .take(10)
//      .foreach(println(_))
  }
}
