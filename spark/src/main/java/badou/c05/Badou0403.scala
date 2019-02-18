package badou.c05

import org.apache.spark.sql.SparkSession

object Badou0403 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("localWC")
      .master("local[4]")
      .getOrCreate()

    val testRdd = spark.sparkContext.textFile("E:\\bigdata\\sophie-1.txt")
    testRdd.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,ascending = false)
      .take(20)
      .foreach(println)
  }

}
