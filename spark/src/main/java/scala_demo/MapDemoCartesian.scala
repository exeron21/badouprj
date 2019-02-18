package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object MapDemoCartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array("tom","tomas", "tomasLee", "tomason"))
    val rdd2 = sc.parallelize(Array("1111","2222", "3333", "4444"))
    var rdd3 = rdd1.cartesian(rdd2)

    rdd3.collect.foreach(println)
  }
}
