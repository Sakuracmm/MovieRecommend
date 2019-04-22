package com.ll.streaming

import com.ll.conf.AppConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkDirectStream5 extends AppConf with Serializable {

  def main(args: Array[String]): Unit = {

    val batchDuration = new Duration(5000)

    @transient
    val ssc = new StreamingContext(spark.sparkContext,batchDuration)

    val validusers = spark.sql("select * from trainingdataasc")
    val userlist = validusers.select("userId")

    val modelpath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/2.3171297248391123"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    //    val broker = "master:9092"
    val topics = "real-time".split(",").toSet

    def exist(u: Int): Boolean = {
      val userlist = spark.sql("select distinct(userid) from trainingdataasc").rdd.map(x => x.getInt(0)).collect()
      userlist.contains(u)
    }

    val defaultrecresult = spark.sql("select * from top5DF").rdd.toLocalIterator

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))


    val messages = kafkaDirectStream.foreachRDD { rdd =>

      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelpath)
      val userrdd = rdd.map(x => x.value().split("|")).map(x => x(1)).map(_.toInt)
      val validusers = userrdd.filter(user => exist(user))
      val newusers = userrdd.filter(user => !exist(user))

      val validusersIter = validusers.toLocalIterator
      val newusersIter = newusers.toLocalIterator
      while (validusersIter.hasNext) {
        val recresult = model.recommendProducts(validusersIter.next, 5)
        println("below movies are recommended for you :")
        println(recresult)
      }
      while (newusersIter.hasNext) {
        println("below movies are recommended for you :")
        for (i <- defaultrecresult) {
          println(i.getString(0))
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()



  }

}
