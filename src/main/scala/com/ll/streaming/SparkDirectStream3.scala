package com.ll.streaming

import com.ll.caseclass.Result
import com.ll.conf.AppConf
import com.ll.ml.Recommender.{jdbcUrl, prop, recResultTable}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.serializer.KryoSerializer

//using
object SparkDirectStream3{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaProducer")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.master","spark://slave1:7077")
      .config("spark.default.parallelism",400)
      .enableHiveSupport()
      .getOrCreate()

    val batchDuration = new Duration(15000)
    val ssc = new StreamingContext(spark.sparkContext,batchDuration)


    val validusers = spark.sql("select * from new_trainingdata")
    val userlist = validusers.select("userId")

    val modelpath = "hdfs://slave1:9000/newMovieRecommendData/bestModel/0.4672721037110623"

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
      val userlist = spark.sql("select distinct(userid) from new_trainingdata").rdd.map(x => x.getInt(0)).collect()
      userlist.contains(u)
    }

    val defaultrecresult = spark.sql("select * from top5DF").rdd.toLocalIterator
    val userslist = spark.sql("select distinct(userid) from new_trainingdata").rdd.map(x => x.getInt(0)).collect()

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))


    val messages = kafkaDirectStream.foreachRDD { rdd =>

      val values: RDD[(String, String)] = rdd.map(rdd => (rdd.key(),rdd.value()))
//      println(values.collect().foreach(x => println("line 64" + x)))


//      println(rdd.collect().foreach(x => println("line 66" + x)))

      val key: RDD[String] = rdd.map(x => x.key())
      //["xxx","yyyy"]
//      val strings: Array[String] = key.collect()
//      println(key.collect().foreach(x => println("line 72" + x)))

      val value: RDD[String] = rdd.map(x => x.value())
//      println(value.collect().foreach(x => println("line 75" + x)))

//      val userrdd = rdd.map(x => x.value().split("|")).map(x => x(1)).map(_.toInt)
//      println(values.map(x => println("++++++++++++++++++++++++line 78  +++" + x._2)))

      val userrdd:RDD[Int] = values.map(x => x._2.split("%")).map(x => x(1)).map(_.toInt)

      userrdd.collect().foreach(x => {
//        println("**************************** line 82   **" + x)
        if(userslist.contains(x)) {
          val recresult: Array[Rating] = model.recommendProducts(x, 5)

          import spark.implicits._
          val rec1 = spark.sparkContext.parallelize(recresult,8)
          val rec2: DataFrame = rec1.map(x=>Result(x.user.toInt,x.product.toInt,x.rating.toDouble)).toDF
          rec2.write.mode(SaveMode.Append).jdbc(jdbcUrl,recResultTable, prop)

          val moviesid: Array[Int] = recresult.map(_.product)
          println(s"用户$x below movies are recommended for you :")
          moviesid.map(x => {
            val moviename = spark.sql(s"select title from new_movies where movieId = $x").first().getString(0)
            //            val moviedetail = spark.sql(s"insert into rec_movie select movieId, title, genres from movies where movieId  = $x")
            println(moviename)
            Unit
          })
        }
        else{
            println(s"用户$x below movies are recommended for you :")
            val moviesTitle = spark.sql("select * from top5df").rdd.map(x => x.get(1).toString).collect()
            val movieid = spark.sql("select * from top5df").rdd.map(x => x.getInt(0)).collect()
            for(i <- moviesTitle){
//              Result(x,movieid,0.0).
              println(i)
            }
            for(i <- movieid){
              import spark.sqlContext.implicits._
              val moviess: DataFrame = spark.sql(s"select movieid from new_movies where movieid=$i").toDF().map(x => x.getInt(0)).map(rec => Result(rec,x,0.0)).toDF()
              moviess.write.mode(SaveMode.Append).jdbc(jdbcUrl,recResultTable,prop)
            }
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
