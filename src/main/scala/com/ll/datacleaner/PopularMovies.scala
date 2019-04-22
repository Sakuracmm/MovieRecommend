package com.ll.datacleaner

import com.ll.conf.AppConf

object PopularMovies extends AppConf{

  def main(args: Array[String]): Unit = {


  val moviesRatingCount = spark.sql("select count(*) c, movieid from trainingdataasc group by movieid order by c desc")
  import spark.sqlContext.implicits._

  spark.sql("create table if not exists top5DF(title string)")
  val top5moviesid: Array[Int] = moviesRatingCount.limit(5).map(x => x.getInt(1)).collect()
  for(i <- top5moviesid){
    spark.sql(s"insert into table top5DF select title from movies where movieid=$i")
  }


}


//  top5movies.createTempView("top5")
//  val top5DF = spark.sql("select title from movies a join top5 b on a.movieid=b.movieid")
//  top5DF.createTempView("top5mdf")
//  //最终的表应该是5部默认排序的电影名称
//  spark.sql("create table if not exists top5DF(title string)")
//  spark.sql(s"insert into table top5DF select title from top5mdf")


}
