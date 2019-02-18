package scala_demo

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object StartModeWordCount {
  var count = 0
  def main(args: Array[String]): Unit = {
    // 创建spark配置对象
    val conf = new SparkConf
    conf.setAppName("wordcountapp")
    conf.setMaster("local[2,3]")

    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = rdd1.map(e => {
      val tname = Thread.currentThread().getName
      println(tname + " : " + e)
      if (count < 3) {
        count +=1
        throw new Exception("xclkvj")
      }
      else
        e
    })

    println(rdd2.reduce(_+_))

  }
}
