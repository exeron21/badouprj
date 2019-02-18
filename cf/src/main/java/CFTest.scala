package main.java

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CFTest {
  case class rating(user_id:String, item_id:String, rating: Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CFTest")
//      .enableHiveSupport()
      .getOrCreate()
    val customSchema = StructType(
      List(
        StructField("user_id", StringType, true),
        StructField("item_id", StringType, true),
        StructField("rating", StringType, true)
      )
    )
    val df = spark.read.format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .schema(customSchema)
        .load("E:/data/rating")
    df.createOrReplaceTempView("table")
    df.show()
    val x = df.select("user_id")
    x.show()
  }
}
