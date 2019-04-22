package com.ll.datacleaner

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive._
import org.apache.spark.mllib.recommendation._

object Ratingdata3 {


  object RatingData {
    def main(args: Array[String]) {
      val localClusterURL = "local[2]"
      val clusterMasterURL = "spark://master:7077"
      val conf = new SparkConf().setAppName("ETL").setMaster(clusterMasterURL)
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val hc = new HiveContext(sc)

      //RDD[Rating]需要从原始表中提取userid,movieid,rating数据
      //并把这些数据切分成训练集和测试集数据
      val ratings = hc.sql("cache table ratings")
      val count = hc.sql("select count(*) from ratings").first().getLong(0).toInt
      val percent = 0.6
      val trainingdatacount = (count * percent).toInt
      val testdatacount = (count * (1 - percent)).toInt

      //用scala feature:String Interpolation来往SQL语句中传递参数
      //order by limit的时候，需要注意OOM的问题
      val trainingDataAsc = hc.sql(s"select userId,movieId,rating from ratings order by timestamp asc")
      trainingDataAsc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataAsc")
      hc.sql("drop table if exists trainingDataAsc")
      hc.sql("create table if not exists trainingDataAsc(userId int,movieId int,rating double) stored as parquet")
      hc.sql("load data inpath '/tmp/trainingDataAsc' overwrite into table trainingDataAsc")

      val trainingDataDesc = hc.sql(s"select userId,movieId,rating from ratings order by timestamp desc")
      trainingDataDesc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataDesc")
      hc.sql("drop table if exists trainingDataDesc")
      hc.sql("create table if not exists trainingDataDesc(userId int,movieId int,ratings double) stored as parquet")
      hc.sql("load data inpath '/tmp/trainingDataDesc' overwrite into table trainingDataDesc")

      val trainingData = hc.sql(s"select * from trainingDataAsc limit $trainingdatacount")
      trainingData.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingData")
      hc.sql("drop table if exists trainingData")
      hc.sql("create table if not exists trainingData(userId int,movieId int,rating double) stored as parquet")
      hc.sql("load data inpath '/tmp/trainingData' overwrite into table trainingData")

      val testData = hc.sql(s"select * from trainingDataDesc limit $testdatacount")
      testData.write.mode(SaveMode.Overwrite).parquet("/tmp/testData")
      hc.sql("drop table if exists testData")
      hc.sql("create table if not exists testData(userId int,movieId int,rating double) stored as parquet")
      hc.sql("load data inpath '/tmp/testData' overwrite into table testData")

      val ratingRDD = hc.sql("select * from trainingData").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getDouble(2)))

      val model = ALS.train(ratingRDD, 1, 10)
    }
  }

}
