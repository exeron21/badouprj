package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object ActionDemoTake {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("E:\\spark_test_file\\datalean.txt")
    rdd1.flatMap(_.split("\t")).map((_,1)).countByKey().foreach(println)
//    rdd2.saveAsTextFile("D:\\spark_test_file\\saveAsFile")
//    rdd2.saveAsSequenceFile("D:\\spark_test_file\\saveAsSequenceFile")
  }
}
