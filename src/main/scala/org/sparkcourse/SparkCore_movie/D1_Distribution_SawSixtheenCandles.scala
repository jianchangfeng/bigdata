package org.sparkcourse.SparkCore_movie

import org.apache.spark.{SparkConf, SparkContext}

object D1_Distribution_SawSixtheenCandles {
  def main(args: Array[String]): Unit = {

    // 创建sparkcontext
    val master = if (args.length > 0) args(0).toString else "local"
    val conf = new SparkConf().setMaster(master).setAppName("MovieUser").set("spark.io.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    // 输入数据
    val usersRdd = sc.textFile("data/ml-1m/users.dat")
    val ratingsRdd = sc.textFile("data/ml-1m/ratings.dat")
    val moviesRdd = sc.textFile("data/ml-1m/movies.dat")

    // 抽取数据的属性，过滤符合条件的电影
    // RDD[(userID, (gender, age))]
    val users = usersRdd.map(_.split("::")).map(x => {
      (x(0), (x(1), x(2)))
    })
    // RDD[(userID, movieID)]
    val rating = ratingsRdd.map(_.split("::")).map(x =>
      (x(0), x(1))).filter(x => x._2.equals("2144"))

    // RDD[(movieID,userID)]
    val rating_2 = ratingsRdd.map(_.split("::")).map(x =>
      (x(1), x(0))).filter(x => x._1.equals("2144"))
    // RDD[(movieID, movieName)]
    val movie = moviesRdd.map(_.split("::")).map(x=>(x(0),x(1)))

    //先获取movieID 对应的movieName
    //RDD[movieID,(UserID,movieName)]
    //(1655,(2144,Phantoms (1998)))
    val movieRating2 = rating_2.join(movie)
    movieRating2.take(1).foreach(println(_))

//    val rating_3 = movieRating2.take(1).foreach(x=>x(1))
    val rating_3 = movieRating2.map(x=>{(x._2._1,x._2._2)})
    rating_3.take(10).foreach(println(_))


    // join两个数据集
    // (4425,(2144,(M,35)))
    // RDD[(userID, (movieID, (gender, age)))]
    val userRating = rating_3.join(users)
    userRating.take(11).foreach(println(_))

    // 统计分析
    val userDistribution = userRating.map(x => {
      ((x._2._1,x._2._2), 1)
    }).reduceByKey(_+_)
      .foreach(println(_))

    sc.stop()
  }

}
