package com.ll.ml

import com.ll.conf.AppConf
import org.apache.spark.ml._
import org.apache.spark.ml.recommendation._


object PipleLine extends AppConf{

  def main(args: Array[String]): Unit = {
    val trainingdata = spark.sql("select * from trainingdataasc")
    val pipeline = new Pipeline().setStages(Array())
  }




}
