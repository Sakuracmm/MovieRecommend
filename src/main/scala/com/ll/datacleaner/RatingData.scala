package com.ll.datacleaner

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object RatingData {
  def main(args: Array[String]): Unit = {
    //本地模式运行
    val localClusterURL = "local[2]"
    //集群模式运行
    val clusterMasterURL = "spark://slave1:7077"

    val conf = new SparkConf().setAppName("RatingData")
      .set("spark.network.timeout","3000")
      .set("spark.executor.heartbeatInterval","1000")
    //      .set("spark.testing.memory", "2147480000")
    //      .setMaster(localClusterURL)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)

    //RDD[Rating]需要从原始表中提取userId，movieId，rating数据
    //并把这些数据切分成训练集和测试集数据，最后一个字段m_timestamp作为一个排序的标准
    val ratings: DataFrame = hc.sql("cache table ratings")
    //返回的一个DataFrame实际上是通过计算后的一个值（这里是ratings表中的行数），取结果的第一行的值得到他的总量
    val count: Int = hc.sql("select count(*) from ratings").first().getLong(0).toInt
    //定义数据比约为6:4
    var percent: Double = 0.6
    //训练数据
    val trainingDataCount: Int = (count * percent).toInt
    //测试数据
    val testDataCount: Int = (count * (1 - percent)).toInt

    //用scala feature：String Interpolation来往SQL中传递参数
    //训练集数据表创建

    //这个语句有问题，会非常消耗内存
    //order by limit的时候，需要注意OOM的问题     解决方法1、拆分 2、扩大内存
    //val trainingDataAsc: DataFrame = hc.sql(s"select userId, movieId, rating from ratings order by m_timestamp asc limit $trainingDataCount")
    //升序排列
    val trainingDataAscTmp: DataFrame = hc.sql("select * from ratings order by m_timestamp asc")
    trainingDataAscTmp.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataAscTmp")
    hc.sql("drop table if exists trainingDataAscTmp")
    hc.sql("create table if not exists trainingDataAscTmp(userId int, movieId int, rating double, m_timestamp int) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataAscTmp' overwrite into table trainingDataAscTmp")
    val trainingDataAsc: DataFrame = hc.sql("select userId, movieId, ratting from trainingDataAscTmp")
    trainingDataAsc.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataAsc")
    hc.sql("drop table if exists trainingDataAsc")
    hc.sql("create table if not exists trainingDataAsc(userId int, movieId int, rating double) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataAsc' overwrite into table trainingDataAsc")

    //降序排列
    val trainingDataDescTmp: DataFrame = hc.sql(s"select userId, movieId, rating, m_timestamp from ratings order by m_timestamp desc")
    trainingDataDescTmp.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataDescTmp")
    hc.sql("drop table if exists trainingDataDescTmp")
    hc.sql("create table if not exists trainingDataDescTmp(userId int, movieId int, rating double, m_timestamp) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataDescTmp' overwrite into table trainingDataDescTmp")
    val trainingDataDesc: DataFrame = hc.sql("select userId, movieId, ratting from trainingDataDescTmp")
    trainingDataDesc.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataDesc")
    hc.sql("drop table if exists trainingDataDesc")
    hc.sql("create table if not exists trainingDataDesc(userId int, movieId int, rating double) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataDesc' overwrite into table trainingDataDesc")

    //trainingData存表
    val trainingData = hc.sql(s"select * from trainingDataAsc limit $trainingDataCount")
    trainingData.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingData")
    hc.sql("drop table if exists trainingData")
    hc.sql("create table if not exists trainingData(userId int, movieId int, rating double) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingData' overwrite into table trainingData")
    //testData存表
    val testData = hc.sql(s"select * from trainingDataDesc limit $testDataCount")
    testData.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/testData")
    hc.sql("drop table if exists testData")
    hc.sql("create table if not exists testData(userId int, movieId int, rating double) stored as parquet")
    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/testData' overwrite into table testData")

    val ratingRDD: RDD[Rating] = hc.sql("select * from trainingData").rdd.map(x => Rating(x.getInt(0), x.getInt(1),x.getDouble(2)))

    val model = ALS.train(ratingRDD, 1, 10)

  }
}