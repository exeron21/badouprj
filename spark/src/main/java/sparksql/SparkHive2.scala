package sparksql

import org.apache.spark.sql.SparkSession

object SparkHive2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("sparkHive2")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()

    spark.sql("show databases").show


    spark.sql("create table if not exists badou.sparkhive1(id int ,name string)")

    val data = Array((1, "jack"), (2, "marry"), (3, "john"))
    val df = spark.createDataFrame(data).toDF("id", "name")
    df.createOrReplaceTempView("tmp1")
    spark.sql("insert into badou.sparkhive1 select id, name from tmp1")
    spark.sql("select * from badou.sparkhive1").show

  }
}
