import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CFTest1 {

  case class rating(user_id: String, item_id: String, rating: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CFTest")
      .getOrCreate()

    val df = spark.read
//      .option("inferSchema", "true")
//      .option("header", "true")
      .option("delimiter", "\t")
      .csv("E:\\badouprj\\python-module\\data\\rating_mini")
      .toDF("user_id", "item_id", "rating")
    df.show(df.count().toInt)

    println("====================")

    val df2 = df.selectExpr("user_id as user_v", "item_id as item_id", "rating as rat_v")
    df2.show(df2.count().toInt)

    println("====================")

    val df3 = df.join(df2, "item_id")
    df3.select("user_id", "user_v").filter("user_id<> user_v").distinct().show(df3.count().toInt)  // 默认显示20条，别被这个骗了
    df3.groupBy("user_id").agg("user_id" -> "count").show()
  }
}
