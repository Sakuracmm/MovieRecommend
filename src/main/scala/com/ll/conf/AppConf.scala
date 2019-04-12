package com.ll.conf

import org.apache.spark.sql.SparkSession

trait AppConf {

  val spark = SparkSession.builder()
    .config("spark.default.parallelism",400)
    .enableHiveSupport()
    .getOrCreate()

}
