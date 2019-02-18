package rdd_trans

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("aggregateByKey1")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\spark_test_file\\hello.txt")
    val rdd2 = rdd1.flatMap(_.split("|"))
    val rdd3 = rdd2.map((_,1))

    def seq(a:Int, b:Int) :Int = {
      math.max(a,b)
    }

    def comb(a:Int , b:Int) :Int = {
      a + b
    }

    rdd3.aggregateByKey(3)(seq, (a:Int, b:Int)=> {
      a + b
    }:Int).foreach(println)
  }
}
