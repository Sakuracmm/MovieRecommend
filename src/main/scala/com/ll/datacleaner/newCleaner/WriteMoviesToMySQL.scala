package com.ll.datacleaner.newCleaner

import java.util

import com.ll.caseclass.newData.Movies
import com.ll.conf.AppConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object WriteMoviesToMySQL extends AppConf{

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val moviesDetail: DataFrame = spark.sql(s"select * from new_movies")
      .rdd
      .map(x => Movies(x.getInt(0),x.getString(1),x.getString(2)))
      .toDF()

    moviesDetail.write.mode(SaveMode.Append).jdbc(jdbcUrl,"movies",prop)
  }

}
