package com.ll.streaming

object myTest {

  def main(args: Array[String]): Unit = {
    val m = 1
    val n = 2
    val i = 109
    val it: Iterator[Int] = Array(1,2,3,4,5).toIterator
    while(it.hasNext) {
      val i = it.next()
      if(add(i,n) != 0) {
        val j = add(m, i)
        println(j)
      }
    }
    def add(a:Int, b:Int): Int ={
      b
    }
  }


}