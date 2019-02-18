package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object DataLeanDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\spark_test_file\\datalean.txt",4) // 这里需要多个分区和多个线程，不然看不到效果
    val rdd2 = rdd1.flatMap(_.split("\t")).map((_,1)).map(t => {
      val word = t._1
      val r = util.Random.nextInt(100)
      (word + '_' + r, 1)
    }).reduceByKey(_ + _).map(t => {
      val word = t._1
      val count = t._2
      val w = word.lastIndexOf("_")
      val m = word.substring(0, w)
      (m , count)
    }).reduceByKey(_+_).saveAsTextFile("D:/spark_test_file/datalean_result")
  }
}
