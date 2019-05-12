package interview

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Problem0511_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    joinRdd(sc)
  }
  def joinRdd(sc:SparkContext): Unit = {
    val name = Array(
      (1, "spark"),
      (2, "tachyon"),
      (3, "hadoop")
    )

    val score = Array(
      (1,100),
      (2,90),
      (3,80),
      (6,93)
    )

    val nameRdd = sc.parallelize(name)
    val scoreRdd = sc.parallelize(score)
    val result = nameRdd.join(scoreRdd)
    result.collect.foreach(println)
  }
}
