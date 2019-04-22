package com.ll.ml

import com.ll.caseclass.Result
import com.ll.conf.AppConf

import java.util.Properties

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}



object RecommendForAllUsers extends AppConf{
  import spark.implicits._
  def main(args: Array[String]): Unit = {

    val users = spark.sql("select distinct(userId) from trainingDataAsc order by userId asc")
    val allusers = users.rdd.map(x => x.getInt(0)).toLocalIterator

    //1、可行，效率不高
    val modelpath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/2.3171297248391123"
    val model = MatrixFactorizationModel.load(spark.sparkContext, modelpath)

    while (allusers.hasNext) {
      val id = allusers.next()
      val rec: Array[Rating] = model.recommendProducts(id, 5)
      writeRecResultToMysql(rec)
//      writeRecResultToHive(rec)
    }
    //2
    //直接给所有用户推荐,不可行，需要的内存过大
    //    val recResult = model.recommendProductsForUsers(5)
  }

  def writeRecResultToMysql(uid: Array[Rating]): Unit ={

    import spark.sqlContext.implicits._
    val uidString  = uid.map(x => x.user.toString + "%"
      + x.product.toString + "%" + x.rating.toString)
    println(uidString)


    val uidDF: DataFrame = spark.sparkContext.parallelize(uidString, 8)
      .map(_.split("%"))
      .map(x => Result(x(0).trim.toInt, x(1).trim.toInt,x(2).trim.toDouble)).toDF()

    uidDF.write.mode(SaveMode.Ignore).jdbc(jdbcUrl, recResultTable, prop)
  }
  //结果写入到数据仓库
//  def writeRecResultToMysql(uid: Array[Rating]): Unit ={}
}
