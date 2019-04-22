package com.ll.ml.newML

import com.ll.conf.AppConf
import org.apache.spark.ml._
import org.apache.spark.ml.recommendation._


object PipleLine extends AppConf{

  //spark.ml是dataframe来构建pipeline，通过pipline来完成机器学习的流水线
  def main(args: Array[String]): Unit = {
    //基于dataFrame来构建Spark MLlib
    val trainingdata = spark.sql("select * from new_trainingdata").withColumnRenamed("userid","user").withColumnRenamed("movieid","item")
    val testData = spark.sql("select * from new_testdata").withColumnRenamed("userid","user").withColumnRenamed("movieid","item")
    trainingdata.cache()
    testData.cache()


    //构建一个estimator
    val als = new ALS().setMaxIter(20).setRegParam(1.0).setRank(1)
    //ml里面的transformer
//    val model = als.fit(trainingdata)
    val p = new Pipeline().setStages(Array(als))
    val model = p.fit(trainingdata)
    val result = model.transform(testData).select("rating","prediction")
    import spark.implicits._
    val MSE = result.map(x =>math.pow(x.getInt(0) - x.getInt(1),2)).rdd.mean()
    val RMSE = math.sqrt(MSE)

    model.save(s"hdfs://slave1:9000/newMovieRecommend/piplelineModel/$RMSE")

  }




}
