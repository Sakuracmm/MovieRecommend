package com.ll.ml.newML

import java.util

import com.ll.conf.AppConf
import com.ll.ml.newML.FeatureExtraction.{Rating, parseRating}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql._

/**
  * Created by manpreet.singh on 07/09/16.
  */
object ALSModeling extends AppConf{

  def createALSModel() {
    val ratings: DataFrame = FeatureExtraction.getFeatures()
//    val ratings: DataFrame = FeatureExtraction.getFeatures()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    println(training.first())

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)
    println(model.userFactors.count())
    println(model.itemFactors.count())

    val predictions = model.transform(test)
    println(predictions.printSchema())

    val rec: DataFrame = model.recommendForAllUsers(10)

    import spark.implicits._
    val rec2: Dataset[(Int, Double)] = rec.map(r => (r.getInt(0),r.getDouble(1)) )
    rec.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")
  }

  def main(args: Array[String]) {
    createALSModel()
  }

}
