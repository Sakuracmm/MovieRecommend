package com.ll.datacleaner

import com.ll.conf.AppConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object RatingData1 extends AppConf{
  def main(args: Array[String]): Unit = {

    spark.conf.set("spark.network.timeout","3000")
    spark.conf.set("spark.executor.heartbeatInterval","1000")

    //RDD[Rating]需要从原始表中提取userId，movieId，rating数据
    //并把这些数据切分成训练集和测试集数据，最后一个字段m_timestamp作为一个排序的标准
    //        val ratings: DataFrame = spark.sql("cache table ratings")
    //返回的一个DataFrame实际上是通过计算后的一个值（这里是ratings表中的行数），取结果的第一行的值得到他的总量
    val count: Int = spark.sql("select count(*) from ratings").first().getLong(0).toInt
    //定义数据比约为6:4
    var percent: Double = 0.6
    //训练数据
    val trainingDataCount: Int = (count * percent).toInt
    //测试数据
    val testDataCount: Int = (count * (1 - percent)).toInt

    //用scala feature：String Interpolation来往SQL中传递参数
    //训练集数据表创建



    val ratingTmp = spark.sql("select * from ratings order by m_timestamp asc")
    ratingTmp.createTempView("v_ratings")
    val ratingTmp2 = spark.sql(s"select userId, movieId, rating, row_number() over(partition by rating order by userId) as rank from v_ratings limit $trainingDataCount")
    ratingTmp2.createTempView("v_ratings2")
    val ratingTmp4 = spark.sql(s"select userId, movieId, rating, rank from v_ratings2 where rank > ${trainingDataCount/3} and rank < ${(trainingDataCount / 3) * 2}")
    ratingTmp4.createTempView("v_ratings4")
    val ratingTmp5 = spark.sql(s"select userId, movieId, rating, rank from v_ratings2 where rank > ${(trainingDataCount / 3) * 2}")
    ratingTmp5.createTempView("v_ratings5")

    spark.sql("drop table if exists trainingDataAsc")
    spark.sql(s"create table trainingDataAsc1 as select userId, movieId, rating from v_ratings2 limit ${trainingDataCount/3}")
    spark.sql(s"insert into table trainingDataAsc1 select userId, movieId, rating from v_ratings4")
    spark.sql(s"insert into table trainingDataAsc1 select userId, movieId, rating from v_ratings5")



    val ratingTmpDesc = spark.sql("select * from ratings order by m_timestamp desc")
    ratingTmpDesc.createTempView("v_ratingDesc")
    val ratingTmpDesc2 = spark.sql(s"select userId, movieId, rating, row_number() over(partition by rating order by userId) as rank from v_ratingDesc limit $testDataCount")
    ratingTmpDesc2.createTempView("v_ratingDesc2")
    val ratingTmpDesc3 = spark.sql(s"select userId, movieId, rating, rank from v_ratingDesc2 where rank > ${testDataCount / 3} and rank < ${(testDataCount / 3) * 2}")
    ratingTmpDesc3.createTempView("v_ratingDesc3")
    val ratingTmpDesc4 = spark.sql(s"select userId, movieId, rating, rank from v_ratingDesc2 where rank > ${(testDataCount / 3) * 2}")
    ratingTmpDesc4.createTempView("v_ratingDesc4")

    spark.sql("drop table if exists testDataDesc")
    spark.sql(s"create table testDataDesc1 as select userId, movieId, rating from v_ratingDesc2 limit ${testDataCount/3}")
    spark.sql(s"insert into table testDataDesc1 select userId, movieId, rating from v_ratingDesc3")
    spark.sql(s"insert into table testDataDesc1 select userId, movieId, rating from v_ratingDesc4")

    //    //本地模式运行
    //    val localClusterURL = "local[2]"
    //    //集群模式运行
    //    val clusterMasterURL = "spark://slave1:7077"
    //
    //    val conf = new SparkConf().setAppName("RatingData")
    //      .set("spark.network.timeout","3000")
    //      .set("spark.executor.heartbeatInterval","1000")
    //    //      .set("spark.testing.memory", "2147480000")
    //    //      .setMaster(localClusterURL)
    //    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    //    val hc = new HiveContext(sc)
    //
    //    //RDD[Rating]需要从原始表中提取userId，movieId，rating数据
    //    //并把这些数据切分成训练集和测试集数据，最后一个字段m_timestamp作为一个排序的标准
    //    val ratings: DataFrame = hc.sql("cache table ratings")
    //    //返回的一个DataFrame实际上是通过计算后的一个值（这里是ratings表中的行数），取结果的第一行的值得到他的总量
    //    val count: Int = hc.sql("select count(*) from ratings").first().getLong(0).toInt
    //    //定义数据比约为6:4
    //    var percent: Double = 0.6
    //    //训练数据
    //    val trainingDataCount: Int = (count * percent).toInt
    //    //测试数据
    //    val testDataCount: Int = (count * (1 - percent)).toInt
    //
    //    //用scala feature：String Interpolation来往SQL中传递参数
    //    //训练集数据表创建
    //
    //    //这个语句有问题，会非常消耗内存
    //    //order by limit的时候，需要注意OOM的问题     解决方法1、拆分 2、扩大内存
    //    //val trainingDataAsc: DataFrame = hc.sql(s"select userId, movieId, rating from ratings order by m_timestamp asc limit $trainingDataCount")
    //    //升序排列
    //    val trainingDataAscTmp: DataFrame = hc.sql("select * from ratings order by m_timestamp asc")
    //    trainingDataAscTmp.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataAscTmp")
    //    hc.sql("drop table if exists trainingDataAscTmp")
    //    hc.sql("create table if not exists trainingDataAscTmp(userId int, movieId int, rating double, m_timestamp int) stored as parquet")
    //    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataAscTmp' overwrite into table trainingDataAscTmp")
    //    val trainingDataAsc: DataFrame = hc.sql("select userId, movieId, ratting from trainingDataAscTmp")
    //    trainingDataAsc.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataAsc")
    //    hc.sql("drop table if exists trainingDataAsc")
    //    hc.sql("create table if not exists trainingDataAsc(userId int, movieId int, rating double) stored as parquet")
    //    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataAsc' overwrite into table trainingDataAsc")
    //
    //    //降序排列
    //    val trainingDataDescTmp: DataFrame = hc.sql(s"select userId, movieId, rating, m_timestamp from ratings order by m_timestamp desc")
    //    trainingDataDescTmp.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataDescTmp")
    //    hc.sql("drop table if exists trainingDataDescTmp")
    //    hc.sql("create table if not exists trainingDataDescTmp(userId int, movieId int, rating double, m_timestamp) stored as parquet")
    //    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataDescTmp' overwrite into table trainingDataDescTmp")
    //    val trainingDataDesc: DataFrame = hc.sql("select userId, movieId, ratting from trainingDataDescTmp")
    //    trainingDataDesc.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingDataDesc")
    //    hc.sql("drop table if exists trainingDataDesc")
    //    hc.sql("create table if not exists trainingDataDesc(userId int, movieId int, rating double) stored as parquet")
    //    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingDataDesc' overwrite into table trainingDataDesc")
    //
    //    //trainingData存表
    //    val trainingData = hc.sql(s"select * from trainingDataAsc limit $trainingDataCount")
    //    trainingData.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/trainingData")
    //    hc.sql("drop table if exists trainingData")
    //    hc.sql("create table if not exists trainingData(userId int, movieId int, rating double) stored as parquet")
    //    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/trainingData' overwrite into table trainingData")
    //    //testData存表
    //    val testData = hc.sql(s"select * from trainingDataDesc limit $testDataCount")
    //    testData.write.mode(SaveMode.Overwrite).parquet("hdfs://slave1:9000/MovieRecommend/tmp/testData")
    //    hc.sql("drop table if exists testData")
    //    hc.sql("create table if not exists testData(userId int, movieId int, rating double) stored as parquet")
    //    hc.sql("load data inpath 'hdfs://slave1:9000/MovieRecommendData/tmp/testData' overwrite into table testData")
    //

    //val ratingRDD: RDD[Rating] = spark.sql("select * from trainingDataAsc").rdd.map(x => Rating(x.getInt(0), x.getInt(1),x.getDouble(2)))

//    val model = ALS.train(ratingRDD, 1, 10)

  }
}
