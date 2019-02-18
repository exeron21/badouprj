package badou.c05

import org.apache.spark.sql.SparkSession

object Badou0401 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("badou0401")
      .master("local[4]")
      .getOrCreate()
    val df = spark.sql("select * from badou.orders")
    import spark.implicits._
    val ons = df.select("user_id","order_number","order_hour_of_day")
      .rdd.map(x=>(x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues({
        _.toArray.sortWith(_._2>_._2)
      }).toDF("user_id","ons")

    // 一个简单的UDF:

    import org.apache.spark.sql.functions._
    val plusUDF = udf((col1:String,col2:String)=> col1.toInt + col2.toInt)
    df.withColumn("plusUDF", plusUDF(col("order_number"),col("order_dow"))).show()
  }
}
