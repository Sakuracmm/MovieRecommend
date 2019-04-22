package com.ll.ml

import com.ll.conf.AppConf
import org.apache.spark.mllib.recommendation._


object Recommender2 extends AppConf{

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("please input one params as the index of user!")
      sys.exit(1)
    }
    //得到训练模型中所有用户的id
    //因为traindata取出来的userid一定都存在于模型中，
    val users = spark.sql("select distinct(userId) from trainingDataAsc order by userId asc")
    val userList = users.toDF().collectAsList()

    //选取随机其中一个用户进行推荐（写死为139位用户）,要求这个用户必须小于users.count()
    val index = args(0).toInt
    if(index > userList.size()){
      System.out.println(s"your params $index is illegal!")
      sys.exit(1)
    }
    val uid = users.take(index).last.getInt(0)

    val modelpath = "hdfs://slave1:9000/MovieRecommendData/tmp/bestModel/0.746510155023553"
    val model = MatrixFactorizationModel.load(spark.sparkContext, modelpath)
    //用户的userid,第二个参数表示要推荐商品的数量
    val rec = model.recommendProducts(uid, 5)
    //对所有Array集合中的元素取出product属性，这里的是电影id属性
    val recmovieid = rec.map(_.product)
    println(s"我为用户 $uid 推荐了以下5部电影：")

//    for(i <- recmovieid){
//      val moviename = spark.sql(s"select title from movies where movieId = $i").first().getString(0)
//      println(moviename)
//    }

//    recmovieid.foreach(x => {
//      val moviename = spark.sql(s"select title from movies where movieId = $x").first().getString(0)
//      println(moviename)
//    })

    spark.sql(s"create table if not exists rec_movie$uid (movieId int, title string, genres string)")
    recmovieid.map(x => {
      val moviename = spark.sql(s"select title from movies where movieId = $x").first().getString(0)
      val moviedetail = spark.sql(s"insert into rec_movie select movieId, title, genres from movies where movieId  = $x")
      println(moviename)
      Unit
    })


  }

}
