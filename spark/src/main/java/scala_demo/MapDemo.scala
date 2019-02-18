package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * created by hl on 2017/3/4
  */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("d:/hello.txt",4)
    val rdd2 = rdd1.flatMap(_.split(" "))
//    val rdd3 = rdd2.map(word=>{println("start");val t=(word,1);println(t + " : end"); t})
//    val rdd3 = rdd2.mapPartitions(it=>{
//      val name = Thread.currentThread().getName
//      println(name + " - partition start")
//      val l = ArrayBuffer[String]()
//      for (e <- it) {
//        l += "_" + e
//      }
//      l.iterator
//    })
      val rdd3 = rdd2.mapPartitionsWithIndex((index,it)=>{
      val name = Thread.currentThread().getName
      println(name + " - " + index + " - partition start")
      val l = ArrayBuffer[String]()
      for (e <- it) {
        l += "_" + e
      }
      l.iterator
    })
    val rdd5 = rdd3.map(word => {
      val name = Thread.currentThread().getName
      println(name + " - map " + word)
      (word, 1)
    })
    val rdd4 = rdd5.reduceByKey(_ + _)
    val r = rdd4.collect

    r.foreach(println)

  }
}
