package com.ll.ml

import com.ll.conf.AppConf
import com.ll.datacleaner.RatingData1.spark
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

object ModelTraining extends AppConf{

  def main(args: Array[String]): Unit = {

    //训练集
    val trainingData = spark.sql("select * from trainingDataAsc")
    val testData = spark.sql("select * from trainingDataDesc")
    val ratingRDD: RDD[Rating] = spark.sql("select * from trainingDataAsc").rdd.map(x => Rating(x.getInt(0), x.getInt(1),x.getDouble(2)))
    val training2 = ratingRDD.map{
      case Rating(userid, movieid, rating) => (userid, movieid)
    }
    val testRDD = testData.rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getInt(2)))
    //实际的评分
    val test2: RDD[((Int,Int), Double)] = testRDD.map{
      case Rating(userid, movieid, rating) => ((userid, movieid), rating)
    }

    //这里需要训练大量的模型才能找到这个合适的模型

    //特征向量的个数
    val rank:Int = 1
    // 归整因子/正则因子
    val lambda:List[Double] = List[Double](0.001,0.005,0.01,0.015,0.02,0.1)
    //迭代次数
    val iteration: List[Int] = List[Int](10,20,30)
    var bestRMSE: Double = Double.MaxValue


    for(l <- lambda; i <- iteration) {
      val model = ALS.train(ratingRDD, rank, i, l)
      val predict = model.predict(training2).map {
        case Rating(userid, movieid, rating) => ((userid, movieid), rating)
      }
      val predictAndFact: RDD[((Int, Int), (Double, Double))] = predict.join(test2)
      val MSE = predictAndFact.map {
        case ((user, product), (r1, r2)) =>
          val err = r1 - r2
          err * err
      }.mean()
      val RMSE = math.sqrt(MSE)

      //RMSE越小代表模型越好或是说越准确
      if(RMSE < bestRMSE){
        model.save(spark.sparkContext, s"hdfs://slave1:9000/MovieRecommendData/tmp/bestRMSE/$RMSE")

        //过拟合，如果训练的结果很完美，完美到脱离了实际

      }
    }
  }

}
