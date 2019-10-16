package org.sparkcourse.SparkCore_movie
import org.apache.spark.{SparkConf, SparkContext}

object D3_Top3PopularMovie {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val conf = new SparkConf().setMaster(master).setAppName("TopKMovie").set("spark.io.compression.codec", "snappy")
    val sc = new SparkContext(conf)

    // 输入
    val ratingsRdd = sc.textFile("data/ml-1m/ratings.dat")
    val moviesRdd = sc.textFile("data/ml-1m/movies.dat")

    // 数据抽取
    val ratings = ratingsRdd.map(_.split("::"))
      .map(x => {
        (x(0), x(1), x(2)) // userid, movieid, rating
      })
    //join movie and rating
    // RDD[(movieID,userID)]
    val rating_2 = ratingsRdd.map(_.split("::")).map(x => (x(1),(x(0),x(2))))
    // RDD[(movieID, movieName)]
    val movie = moviesRdd.map(_.split("::")).map(x=> (x(0),x(1)))

    //先获取movieID 对应的movieName
    //RDD[movieID,(UserID,movieName)]
    //(2828,(16,Dudley Do-Right (1999)))
    val movieRating2 = rating_2.join(movie)
    movieRating2.take(1).foreach(println(_))

    val rating_3 = movieRating2.map(x=>{(x._2._1._1,x._2._2,x._2._1._2)})
    rating_3.take(1).foreach(println(_))


    // 分析
    val topKScoreMostMovie = rating_3.map(x => {
      (x._2, (x._3.toInt, 1))
    }).reduceByKey((v1, v2) => {
      (v1._1 + v2._1, v1._2 + v2._2)
    }).map(x => {
      (x._2._1.toFloat / x._2._2.toFloat, x._1)
    }).sortByKey(false)
      .take(3)
      .foreach(println(_))

    sc.stop()
  }


}
