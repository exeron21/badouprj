package rdd_trans

import org.apache.spark.{SparkConf, SparkContext}


// 分别利用map和mapPartition，将输入的数字a转化为(a, a*2)，如：
// 1,2,3,4,5 => (1,2),(2,4),(3,6),(4,8),(5,10)
// map是对集合的每个元素应用作用参数的那个匿名函数。
// mapPartitions是对每个分区应用匿名函数
object _02_map02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("_02_map_partition")
      .setMaster("local[4]") // 这里的local[4]指的是并发线程数
    val sc = new SparkContext(conf)
    // parallelize的第二个参数是分区数
    var rdd1 = sc.parallelize(1 to 10, 2)
    // 下面不能这样写： rdd.map(_, _*2)
    // 似乎是因为用_代表循环中每个元素，但只能用一次只能使用
    /*var rdd2 = rdd1.map(a => {
      val tname = Thread.currentThread().getName
      println(tname + " : map " + a)
      (a, a * 2)
    })
    rdd2.foreach(println)
    println*/

    var rdd3 = rdd1.mapPartitions(a => {
      println("partition start")
      val tname = Thread.currentThread().getName
      println(tname + " : map " + a)
      var res = List[(Int, Int)]()
      for (e <- a) {
        res .::= (e, e*2)
      }
      res.iterator
    })

    rdd3.foreach(println)
  }
}
