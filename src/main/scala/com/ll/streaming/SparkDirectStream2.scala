package com.ll.streaming

import com.ll.conf.AppConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDirectStream2 extends AppConf{

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
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val modePath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/2.3171297248391123"
    val topics = "real-time".split(",").toSet
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val model = MatrixFactorizationModel.load(spark.sparkContext, modePath)

    kafkaDirectStream.foreachRDD(rdd =>
      rdd.foreachPartition{ partitionIterator =>
          println("######################"+partitionIterator.toString())
          val recordValue: Iterator[String] = partitionIterator.map(r => r.value())
          recordValue.foreach( record => {
            println(record)
            val strings: Array[String] = record.split("|")
            println(strings.toString)
//            val uid: Int = strings(1).toInt
            val result = model.recommendProducts(221089, 2)
            println(result)
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
