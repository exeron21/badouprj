package interview

import org.apache.spark.sql.SparkSession

object Problem0511_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    val rdd1 = spark.sparkContext.textFile("./text")
    val rdd2 = rdd1.flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_+_)
    rdd2.sortBy(_._2,ascending=false).collect.take(10).foreach(println)
    println("=======================")
    rdd2.map(x=>(x._2,x._1)).sortByKey(ascending = false).collect.take(10).foreach(println)
  }

}
