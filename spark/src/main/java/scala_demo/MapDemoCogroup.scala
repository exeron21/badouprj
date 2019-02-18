package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object MapDemoCogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local") //线程
    conf.setAppName("wordcountdemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("d:/stus.txt") //分区
    val rdd2 = sc.textFile("d:/cards.txt") //分区

    val rdd3 = rdd1.map(line => {
      (line.split("\\|")(0), line)
    })
    val rdd4 = rdd2.map(line => {
      (line.split("\\|")(0), line)
    })


    val rdd5 = rdd3.cogroup(rdd4)

    rdd5.foreach(t => {
      println(t._1 + "================")
      t._2._1.foreach(println)
      println("================")
      t._2._2.foreach(println)
    })
  }
}
