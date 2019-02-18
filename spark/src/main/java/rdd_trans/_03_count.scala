package rdd_trans

import org.apache.spark.{SparkConf, SparkContext}

// count的n种方式
object _03_count {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("_02_map_partition")
      .setMaster("local[4]") // 这里的local[4]指的是并发线程数
    val sc = new SparkContext(conf)
    // parallelize的第二个参数是分区数
    val rdd1 = sc.textFile("e:\\spark\\hello.txt")

    // 第0种方法
    val count0 = rdd1.map(_.split(" ").length).reduce(_+_)
    println(count0)

    // 第一种count方法
    val rdd2 = rdd1.flatMap(_.split(" "))
    println(rdd2.count())

    // 第二种
    val count1 = rdd2.count()
    println(count1)

    // 第三种
    val rdd3 = rdd2.map((_, 1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val rdd5 = rdd4.map(_._2)
    val count2 = rdd5.reduce(_ + _)
    println(count2)


  }
}
