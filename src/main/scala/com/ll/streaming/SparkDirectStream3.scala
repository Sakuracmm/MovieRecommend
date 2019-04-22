package com.ll.streaming

import com.ll.conf.AppConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.serializer.KryoSerializer

object SparkDirectStream3{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MovieRecommend").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.master","spark://slave1:7077").config("spark.default.parallelism",400).enableHiveSupport().getOrCreate()

    val batchDuration = new Duration(5000)
    val ssc = new StreamingContext(spark.sparkContext,batchDuration)
    val validusers = spark.sql("select * from trainingdataasc")
    val userlist = validusers.select("userId")

    val modelpath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/0.5863293051866363"
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

    val model = MatrixFactorizationModel.load(ssc.sparkContext, modelpath)


    def exist(u: Int): Boolean = {
      val userlist = spark.sql("select distinct(userid) from trainingdataasc").rdd.map(x => x.getInt(0)).collect()
      userlist.contains(u)
    }

    val defaultrecresult = spark.sql("select * from top5DF").rdd.toLocalIterator
    val userslist = spark.sql("select distinct(userid) from trainingdataasc").rdd.map(x => x.getInt(0)).collect()

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))


    val messages = kafkaDirectStream.foreachRDD { rdd =>

      val values: RDD[(String, String)] = rdd.map(rdd => (rdd.key(),rdd.value()))
      println(values.collect().foreach(x => println("line 53" + x)))


      println(rdd.collect().foreach(x => println("line 56" + x)))

      val key: RDD[String] = rdd.map(x => x.key())
      //["xxx","yyyy"]
//      val strings: Array[String] = key.collect()
      println(key.collect().foreach(x => println("line 61" + x)))

      val value: RDD[String] = rdd.map(x => x.value())
      println(value.collect().foreach(x => println("line 64" + x)))

//      val userrdd = rdd.map(x => x.value().split("|")).map(x => x(1)).map(_.toInt)
      println(values.map(x => println("++++++++++++++++++++++++line 68  +++" + x._2)))

      val userrdd:RDD[Int] = values.map(x => x._2.split("%")).map(x => x(1)).map(_.toInt)
      userrdd.collect().foreach(x => {
          println("**************************** line 71   **" + x)
        if(userslist.contains(x)) {
          val recresult: Array[Rating] = model.recommendProducts(x, 5)
          val moviesid: Array[Int] = recresult.map(_.product)
          println("below movies are recommended for you :")
          moviesid.map(x => {
            val moviename = spark.sql(s"select title from movies where movieId = $x").first().getString(0)
            //            val moviedetail = spark.sql(s"insert into rec_movie select movieId, title, genres from movies where movieId  = $x")
            println(moviename)
            Unit
          })
        }
        else{
          println("below movies are recommended for you :")
          spark.sql("select * from top5df").show()
        }
        }
      )




//      val validusers = userrdd.filter(user => exist(user))
//      val newusers = userrdd.filter(user => !exist(user))

//      val validusersIter = validusers.toLocalIterator
//      val newusersIter = newusers.toLocalIterator
//      while (validusersIter.hasNext) {
//        val recresult = model.recommendProducts(validusersIter.next, 5)
//        println("below movies are recommended for you :")
//        println(recresult)
//      }
//      while (newusersIter.hasNext) {
//        println("below movies are recommended for you :")
//        for (i <- defaultrecresult) {
//          println(i.getString(0))
//        }
//      }
    }
    ssc.start()
    ssc.awaitTermination()



  }

}
