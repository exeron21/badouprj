package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by hl on 2017/3/4
  */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    // 创建spark配置对象
    val conf = new SparkConf()
    // 设置master属性
//    conf.setMaster("local[2]")
    conf.setMaster("local")
    conf.setAppName("wordcountdemo")
//    conf.set("spark.executor.memory", "512M")
    // 通过conf创建sc
    val sc = new SparkContext(conf)
    // spark

    val rdd1 = sc.textFile("e:\\spark\\The_Man_of_Property.txt")

    // flatMap压扁
    val rdd2 = rdd1.flatMap(line => {
      //println(" flatMap = " + line)
      line.split(" ")
    })

    // map变换
    val rdd3 = rdd2.map(word => {
      //println("map::: " + word)
      (word , 2)
    })

    val rdd4 = rdd3.reduceByKey(_+_)

    rdd4.collect().foreach(println)
  }
}
