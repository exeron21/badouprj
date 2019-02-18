package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object MapDemoGroupby {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("d:/stus.txt") //分区
    println("rdd1.count() = " + rdd1.count())
    println("rdd1.count() = " + rdd1.first())
    val rdd2 = rdd1.map(line => {
      val key = line.split("\\|")(3)
      (key, line)
    })
    val rdd3 = rdd2.groupByKey()
    rdd3.collect().foreach(t => {
      val key = t._1
      println(key + " : ===================== ")
      for (e <- t._2) {
        println(e)
      }
    })

    val rdd8 = rdd1.sortBy(_.split("\\|")(1))
    rdd8.foreach(println)
  }
}
