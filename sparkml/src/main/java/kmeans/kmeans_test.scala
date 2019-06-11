package kmeans

import org.apache.spark.sql.SparkSession

object kmeans_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kmeans_test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


  }
}
