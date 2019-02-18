package badou.c05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object WriteToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("writeToMySql")
      .getOrCreate()

    val file = spark.read.format("text").load("E:/bigdata/income.csv")
    val df = file.withColumn("splitcol", split(col("value"),","))
      .select(col("splitcol").getItem(0).as("id"),
        col("splitcol").getItem(1).as("name"),
        col("splitcol").getItem(2).as("phone"),
        col("splitcol").getItem(3).as("income"),
        col("splitcol").getItem(4).as("income_desc"),
        col("splitcol").getItem(5).as("income_date")
      )

    val url = "jdbc:mysql://master:3306/test?characterEncoding=UTF-8"
    val table = "salary"
    val prop = new java.util.Properties()
    prop.setProperty("user", "hdp")
    prop.setProperty("password", "hadoop")
    df.write.mode("append").jdbc(url, table, prop)
  }

}
