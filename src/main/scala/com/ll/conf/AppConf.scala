package com.ll.conf

import java.util.Properties

import org.apache.spark.sql.SparkSession

trait AppConf {

  val spark = SparkSession.builder().config("spark.master","spark://slave1:7077").appName("MovieRecommend").config("spark.default.parallelism",400).enableHiveSupport().getOrCreate()

  //jdbc连接
  val jdbcUrl = "jdbc:mysql://slave3:3306/rec_movies"
  val recResultTable = "user_movies_recommendation"
  val mysqlusername = "root"
  val mysqlpassword = "root"
  val prop = new Properties()
  prop.put("driver","com.mysql.jdbc.Driver")
  prop.put("user",mysqlusername)
  prop.put("password",mysqlpassword)

}
