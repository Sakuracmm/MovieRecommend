package com.ll.datacleaner.newCleaner

import com.ll.conf.AppConf


//using
object RatingData extends AppConf{
  def main(args: Array[String]): Unit = {

    spark.conf.set("spark.network.timeout","3000")
    spark.conf.set("spark.executor.heartbeatInterval","1000")

    //RDD[Rating]需要从原始表中提取userId，movieId，rating数据
    //并把这些数据切分成训练集和测试集数据，最后一个字段m_timestamp作为一个排序的标准
    //        val ratings: DataFrame = spark.sql("cache table ratings")
    //返回的一个DataFrame实际上是通过计算后的一个值（这里是ratings表中的行数），取结果的第一行的值得到他的总量
    val count: Int = spark.sql("select count(*) from new_ratings").first().getLong(0).toInt
    //定义数据比约为6:4
    var percent: Double = 0.6
    //训练数据
    val trainingDataCount: Int = (count * percent).toInt
    //测试数据
    val testDataCount: Int = (count * (1 - percent)).toInt

    //用scala feature：String Interpolation来往SQL中传递参数
    //训练集数据表创建


    //ratings
    val ratingTmp = spark.sql("select * from new_ratings order by timestamp asc")
    ratingTmp.createTempView("v_ratings")
    val ratingTmp2 = spark.sql(s"select userId, movieId, rating from v_ratings limit $trainingDataCount")
    ratingTmp2.createTempView("v_ratings2")

    spark.sql("drop table if exists new_trainingdata")
    spark.sql(s"create table new_trainingdata as select userId, movieId, rating from v_ratings2")


    val ratingTmpDesc = spark.sql("select * from new_ratings order by timestamp desc")
    ratingTmpDesc.createTempView("v_ratingDesc")
    val ratingTmpDesc2 = spark.sql(s"select userId, movieId, rating from v_ratingDesc limit $testDataCount")
    ratingTmpDesc2.createTempView("v_ratingDesc2")


    spark.sql("drop table if exists new_testdata")
    spark.sql(s"create table new_testdata as select userId, movieId, rating from v_ratingDesc2")

  }
}
