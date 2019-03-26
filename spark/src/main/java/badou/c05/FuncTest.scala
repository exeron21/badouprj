package badou.c05

import org.apache.spark.sql.SparkSession

object FuncTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("FuncTest")
      .enableHiveSupport()
      .getOrCreate()

    val o = spark.sql("select * from badou.orders")
//    val p = spark.sql("select * from badou.priors")

    import spark.implicits._
    val op = o.select("user_id", "order_number", "order_hour_of_day").rdd.map(x=>(x(0).toString, (x(1).toString, x(2).toString))).groupByKey().mapValues{_.toArray.sortWith(_._2>_._2)}.toDF
    op.show(100)

    import org.apache.spark.sql.functions._
    val plusUDF = udf((a:String, b:String) => {a.toInt+b.toInt})
    // 使用udf不能用spark.select("*", plusUDF(col("order_number"), col("order_dow")))这种方式
    // 只能用withColumn("new_column_name", plusUDF(col("order_number"), col("order_dow")))
    val dftmp = o.withColumn("order_id", plusUDF(col("order_number"), col("order_dow")))
  }
}
