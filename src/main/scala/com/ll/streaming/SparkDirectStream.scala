package com.ll.streaming


import com.ll.conf.AppConf

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object SparkDirectStream extends AppConf{

  def exist(user: Int): Boolean = {
    val userId: Array[Int] = spark.sql("select distinct(userid) from trainingdataasc1").rdd.map(x => x.getInt(0)).collect()
    userId.contains(user)
  }

  //为没有登录的用户推荐电影的策略
  //1、推荐观看人数较多的电影 √
  //2、推荐最新的电影
  def recommendPopularMovies(): Unit = {
    spark.sql("select * from top5df").show()
  }

  def main(args: Array[String]): Unit = {

    //每五秒执行一次到RDD的转换
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
//    val validusers: DataFrame = spark.sql("select * from trainingdataasc1")
//    val userlist = validusers.select("userid")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val modePath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/2.3171297248391123"
    //消费单个topic
    val topics = "real-time".split(",").toSet



    //创建SparkStreaming接收kafka消息队列数据的2种方式
    //一种是Direct Approache,通过SparkStreaming自己主动去Kafka的消息队列中查询还没有接收进来的，
    // 并把它们拿到SparkStreaming中（pull的方式）
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val model = MatrixFactorizationModel.load(spark.sparkContext, modePath)
    val messages = kafkaDirectStream.foreachRDD { rdd =>
      //      val userrdd: Unit = rdd.map(x => x.value().split("|")).map(x => x(1))
      //对每一个rdd加载这个模型，然后产生推荐结果
      //      rdd.foreach{record =>
      //        val user = record.value().split("|").apply(1).toInt
      //        val model = MatrixFactorizationModel.load(spark.sparkContext,modePath)
      //        //给出五个推荐结果
      //        val recresult = model.recommendProducts(user,5)
      //        println(recresult)
      //      }
//      var user = 0
//      rdd.foreachPartition{ partition =>
//        partition.foreach(p => {
//          val records: Array[String] = p.value().split("|")
      val value: RDD[String] = rdd.map(r => r.value())
      println(value.toString())
      val str: String = value.toString()
      println(str)
      val records: Array[String] = str.split("|")
      println(records)
      val uid: Int = records(1).toInt
      val recs: Array[Rating] = model.recommendProducts(uid, 5)
      println("below movies are recommend for you:")
      println(recs)
//        })
//        var userList: Iterator[Int] = partition.map(x => x.value().split("|")).map(x => x(1).toInt)
//        while (userList.hasNext) {
//
//          user = userList.next()
//
////          if (exist(user)) {
//            val recresult = model.recommendProducts(user, 5)
//            println("below movies are recommend for you:")
//            println(recresult)
////          } else {
////            println("below movies are recommend for you:")
////            recommendPopularMovies()
////          }
//        }
      }

    ssc.start()
    ssc.awaitTermination()
  }


}
