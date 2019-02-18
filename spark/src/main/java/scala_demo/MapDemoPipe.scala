package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object MapDemoPipe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array("d:\\"))
    val rdd2 = rdd1.pipe("dir d:\\")
    rdd2.collect().foreach(println)
  }
}
