package rdd_trans

import org.apache.spark.{SparkConf, SparkContext}

// count的n种方式
object _04_groupby {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("_02_map_partition")
      .setMaster("local[4]") // 这里的local[4]指的是并发线程数
    val sc = new SparkContext(conf)
    // parallelize的第二个参数是分区数
    val rdd1 = sc.textFile("e:\\spark\\stu.txt", 4)

    val rdd2 = rdd1.map(line => {
      val words = line.split(" ")
      (words(2), line)
    })

    val rdd3 = rdd2.groupByKey()
    // 像map,foreach这种高阶函数，只能接受简单的匿名函数做为参数，此时可以用_(下划线)代替每个元素：
    // rdd1.map(_.split(",")
    // 如果较为复杂的处理，则需要用 e => { 。。。 } 这种方式来调用，不能再使用_(下划线)来代替。
/*    rdd3.collect().foreach({
      println(_._1)
    })*/
    /*    rdd3.collect().foreach(e => {
          println(e._1 + "====")
          for (i<-e._2)
            println(i)
        })*/
  }
}
