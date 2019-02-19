package rdd_trans

import org.apache.spark.{SparkConf, SparkContext}

object _01_lazy_calucation_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("aggregateByKey1")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("e:\\spark\\hello.txt")
    val rdd2 = rdd1.flatMap(line => {
      println("flatMap : " + line)
      line.split("\\|")
    })
    var rdd3 = rdd2.map((_,1))
    var rdd4 = rdd3.reduceByKey(_ + _)
    var r = rdd4.collect()
    r.foreach(println)
  }
}
