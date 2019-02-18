package rdd_trans

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object _02_map_partition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("_02_map_partition")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("e:\\spark\\hello.txt", 4)
    var rdd2 = rdd1.flatMap(_.split(" "))
    /*val rdd3 = rdd2.map(word => {
      println("start")
      val t = (word, 1)
      println(t + "end")
      t
    })*/

    // mapPartition 有一个迭代器为参数，返回另一个迭代器
    val rdd3 = rdd2.mapPartitions(it => {
      val buf = ArrayBuffer[String]()
      println("partitions start")
      for (e <- it) {
        buf.+=("_" + e)
      }
      buf.iterator
    })
    val rdd0 = rdd3.map((_, 1))
    val rdd4 = rdd0.reduceByKey(_ + _)
    rdd4.collect().foreach(println)
  }
}
