package com.ll.ml.newML

import com.ll.conf.AppConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd._

import scala.math._

//using
object ModelTraining extends AppConf{

  def main(args: Array[String]): Unit = {
    //训练集
    val trainingData = spark.sql("select * from new_trainingdata")
    val testData = spark.sql("select * from new_testdata")

    val ratingRDD: RDD[Rating] = spark.sql("select * from new_trainingdata").rdd.map(x => Rating(x.getInt(0), x.getInt(1),x.getDouble(2)))
    val training2 = ratingRDD.map{
      case Rating(userid, movieid, rating) => (userid, movieid)
    }
    val testRDD = testData.rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getDouble(2)))
    //实际的评分
    val test2: RDD[((Int,Int), Double)] = testRDD.map{
      case Rating(userid, movieid, rating) => ((userid, movieid), rating)
    }

    //这里需要训练大量的模型才能找到这个合适的模型

    //特征向量的个数
//    val rank:Int = 10
    val rank:Int = 10
    // 归整因子/正则因子，值越大越不容易产生过拟合现象，但是值越大会降低值的精准度
//    val lambda:List[Double] = List[Double](0.005,0.01,0.015,0.02,0.04,0.07,0.1,0.2)
    val lambda:List[Double] = List[Double](0.07)
//    val lambda:List[Double] = List[Double](0.0015)
    //迭代次数
    val iteration: List[Int] = List[Int](10)
//    val iteration: List[Int] = List[Int](20)
    var bestRMSE: Double = Double.MaxValue
    var bestIteration = 0
    var bestLambda = 0.0


    //持久化RDD，以便接下来的循环训练执行速度提升
    ratingRDD.persist()
    test2.persist()
    training2.persist()
    //训练多个模型，并且所采用的参数不一样，以找到最为合适的参数
    //双层循环，训练来找到最合适的参数
    //会为每种lambda参数和iteration参数做组合，然后产生RMSE，得到其中最优解
    //当迭代模型越来越多，结果值会愈发收敛

    for(l <- lambda; i <- iteration) {
      val model = ALS.train(ratingRDD, rank, i, l)
      //通过模型训练后预测的参数
      val predict = model.predict(training2).map {
        case Rating(userid, movieid, rating) => ((userid, movieid), rating)
      }
      val predictAndFact = predict.join(test2)
      val MSE = predictAndFact.map {
        case ((user, product), (r1, r2)) =>
          val err = r1 - r2
          err * err
      }.mean()
      val RMSE = math.sqrt(MSE)
      //将得到的RMSE存放在不同的文件夹
      model.save(spark.sparkContext, s"hdfs://slave1:9000/newMovieRecommendData/bestModel/$RMSE")
      //RMSE越小代表模型越好或是说越准确
      // 过拟合现象，如果训练的结果很完美，完美到脱离了实际
      //最终会得到RMSE最小也就是说最合适的结果的各项参数
      if(RMSE < bestRMSE){
        //如果找到了更小的值，作替换
        bestRMSE = RMSE
        bestIteration = i
        bestLambda = l
      }
      println(s"Best mode is located in hdfs://slave1:9000/newMovieRecommendData/bestModel/$RMSE")
      println(s"Best bestRMSE is $RMSE")
      println(s"Best bestIteration is $bestIteration")
      println(s"Best bestLambda is $bestLambda")
    }
  }

}
