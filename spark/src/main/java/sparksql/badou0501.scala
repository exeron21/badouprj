package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object badou0501 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setJars(List("E:\\IdeaProjects\\hadoop-test\\spark-test\\target\\spark-test-1.0-SNAPSHOT.jar"))
    val spark = SparkSession
      .builder()
      .appName("badou0501")
      .master("spark://master:7077")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from badou.orders").show(20)
//    import spark.implicits._
    val df = spark.sql("select * from badou.orders")
//    val orderNumberSort = df.select("user_id", "order_number","order_hour_of_day").rdd.map(x=>(x(0).toString,(x(1).toString,x(2).toString))).groupByKey.mapValues({_.toArray.sortWith(_._2<_._2)}).toDF("user_id","ons")
//
//    orderNumberSort.show(10)


    import org.apache.spark.sql.functions._

    val plusUDF = udf((a:Int,b:Int)=>a+b)
    df.withColumn("plusUDF", plusUDF(col("order_number"),col("order_dow")))
  }

}
