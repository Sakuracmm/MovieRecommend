package com.ll.streaming

import java.util.Properties

import com.ll.conf.AppConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame

//using
object KafkaProducer2 extends AppConf{

  def main(args: Array[String]): Unit = {

    spark.sparkContext
      .getConf
      .setAppName("KafkaProducer")

    val testDF: DataFrame = spark.sql("select * from new_trainingdata")
    val topic = "real-time"

    val props = new Properties()
    props.setProperty("bootstrap.servers","slave1:9092,slave2:9092,slave3:9092")
    props.setProperty("acks", "all")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

//    val kafkaParams = Map[String,Object]("bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
//      "acks" -> "all",
//      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
//      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
//    )



    import spark.implicits._
    val testData = testDF.map(r => (topic, r.getInt(0).toString + "%" + r.getInt(1).toString + "%" + r.getDouble(2).toString))
    val producer = new KafkaProducer[String,String](props)
    val messages = testData.toLocalIterator()

    while(messages.hasNext){
      val message = messages.next()
      val record = new ProducerRecord[String,String](topic,message._1,message._2)
      println(record)
      producer.send(record)
      Thread.sleep(3000)
    }
    producer.close()

    //用for循环的方式会产生序列化的问题，所以需要改成迭代器的方式
    //    val messages = testData.toLocalIterator()
//    for(x <- testData){
//      val message = x
//      val producer = new KafkaProducer[String,String](props)
//      val record = new ProducerRecord[String,String]("canlie",message.,message._2)
//      producer.send()
//    }
    //为什么不同.map或者foreach方法？是因为这两种方法会让你的数据做分布式计算，在计算的时候，处理数据的顺序是无序的
//    testData.foreach()    //效率高

  }

}
