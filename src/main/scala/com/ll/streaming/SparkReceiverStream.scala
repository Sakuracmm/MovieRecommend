package com.ll.streaming

import com.ll.conf.AppConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkReceiverStream extends AppConf{

  def main(args: Array[String]): Unit = {
    //每五秒执行一次到RDD的转换
    val batchDuration = new Duration(5000)
    val ssc = new StreamingContext(spark.sparkContext,batchDuration)

    val validusers: DataFrame = spark.sql("select * from trainingDataAsc")


    val broker = "slave1:9092,slave2:9092,slave3:9092"
    val kafkaParams = Map[String,Object]("broker" -> broker)
    val modePath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/0.5863293051866363"
    //消费单个topic
    val topics = "rea-time".split(",").toList






  }
}
