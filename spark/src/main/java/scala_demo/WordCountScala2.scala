package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object WordCountScala2 {
  def main(args: Array[String]): Unit = {
    // 创建spark配置对象
    val conf = new SparkConf()
    // 设置master属性
    conf.setMaster("local[2]")
    conf.setAppName("wordcountdemo")
    // 通过conf创建sc
    val sc = new SparkContext(conf)
    // spark

    // 通过conf创建sc
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.flatMap(line => line.split(" "))
    val rdd3 = rdd2.map((_, 1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val r = rdd4.collect
    r.foreach(println)
  }
}
