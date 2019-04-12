package com.ll.datacleaner

import com.ll.caseclass._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.SaveMode

object ETL {

  def main(args: Array[String]): Unit = {
    //本地模式运行
    val localClusterURL = "local[2]"
    //集群模式运行
    val clusterMasterURL = "spark://slave1:7077"

    val conf = new SparkConf().setAppName("ETL")
//      .set("spark.testing.memory", "2147480000")
//      .setMaster(localClusterURL)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)

    import sqlContext.implicits._

    //设置RDD的partition的数量一般以集群分配给CPU核数的整数倍为宜
    val minPartitions = 12
    //通过case class来定义Links的数据结构，数据的schema,适用于schema已知的情况

    val links: DataFrame = sc.textFile("hdfs://slave1:9000/MovieRecommendData/data/links.txt", minPartitions) //e
      .filter(!_.endsWith(",")) //e
      .map(x => {
        val link: Array[String] = x.split(",")
        val links = Links(link(0).trim().toInt, link(1).trim().toInt, link(2).trim().toInt)
        links
      })  //e
      .toDF()
    val movies: DataFrame = sc.textFile("hdfs://slave1:9000/MovieRecommendData/data/movies.txt", minPartitions)
      .filter(!_.endsWith(","))
      .map( x=> {
        val movie: Array[String] = x.split(",")
        val movies: Movies = Movies(movie(0).trim().toInt,movie(1).trim().toString,movie(2).trim().toString)
        movies
      }).toDF()

    val ratings: DataFrame = sc.textFile("hdfs://slave1:9000/MovieRecommendData/data/ratings.txt", minPartitions)
      .filter(!_.endsWith(","))
      .map( x=> {
        val rating: Array[String] = x.split(",")
        val ratings: Ratings = Ratings(rating(0).trim().toInt,rating(1).trim().toInt,rating(2).trim().toDouble,rating(3).trim.toInt)
        ratings
      }).toDF()

    val tags: DataFrame = sc.textFile("hdfs://slave1:9000/MovieRecommendData/data/tags.txt", minPartitions)
      .filter(!_.endsWith(","))
      .map( x=> {
        val xx = rebuild(x)
        val tag: Array[String] = xx.split(",")
        val tags: Tags = Tags(tag(0).trim().toInt,tag(1).trim().toInt,tag(2).trim().toString,tag(3).trim.toInt)
        tags
      }).toDF()

    //写入到数据仓库（用hive构建的）中
    //在SQLContext下面只能构建临时表
    //通过数据写入到HDFS中，将表保存到临时表
    //write专门负责输出动作并且可以定义输出的模式（这里是Override）
    //用parquet格式来保存数据
    //links
    links.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommendData/tmp/links")
    hc.sql("drop table if exists links")
    hc.sql("create table if not exists links(movieId int,imdbId int,tmdbId int) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/links' overwrite into table links")

    //movies
    movies.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommendData/tmp/movies")
    hc.sql("drop table if exists movies")
    hc.sql("create table if not exists movies(movieId int, title string, genres string) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/movies' overwrite into table movies")

    //ratings
    ratings.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommendData/tmp/ratings")
    hc.sql("drop table if exists ratings")
    hc.sql("create table if not exists ratings(userId int, movieId int, rating double, m_timestamp int) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/ratings' overwrite into table ratings")

    //tags
    tags.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommendData/tmp/tags")
    hc.sql("drop table if exists tags")
    hc.sql("create table if not exists tags(userId int, movieId int, tag string, m_timestamp int) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/tags' overwrite into table tags")


  }


  //数据加工，将tage每一行中第三个字段，也就是tags字段中的逗号去掉
  //第二个逗号的右边，最后一个逗号的左边
  private def rebuild(input: String): String = {
    val a = input.split(",")
    //把a的前两个元素，拿出来然后在与“，”拼接成字符串
    //head = a(0),a(1)
    val head = a.take(2).mkString(",")
    //从右边开始取出元素，这里取出来一个
    //tail = a(a.length - 1)
    val tail = a.takeRight(1).mkString
    //取出中间字段也就是数据仓库表中tags字段需要处理的字段
    //逗号已经是没有了的，因为在a.split的时候就已经没有了逗号
    //这里直接把b变成字符串，然后将字符串中的引号去掉
    val b = a.drop(2).dropRight(1).mkString.replace("\"","")
    //拼接字符串
    val output = head + "," + b + "," + tail
    output
  }
}
