package scala_demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object FirstScalaWordCount {
  def main(args:Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf() .setMaster("local") .setAppName("appName"))
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .filter(_.contains("wor")).map((_,1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("master")
      .master("local[*]")
      .getOrCreate()
  }
}
