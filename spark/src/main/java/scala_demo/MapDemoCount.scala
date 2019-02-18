package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object MapDemoCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("d:/hello.txt", 4) //分区
    println(rdd1.map(_.split("|").length).reduce(_ + _))

    println(rdd1.flatMap(_.split("|")).count())

    println(rdd1.flatMap(_.split("|")).map((_, 1)).count())


    val rdd2 = rdd1.flatMap(_.split("\\|")).map((_,1)).reduceByKey(_+_).collect()
    rdd2.foreach(println)
  }
}
