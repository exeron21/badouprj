package sparksql

import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkHive2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("OldSparkHiveDemo")
      .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hiveCtx = new HiveContext(sc)


    hiveCtx.sql("show databases").show

    import spark.sql

    /*
        sql("create table if not exists badou.sparkhive1(id int ,name string)")

        val data = Array((1, "jack"), (2, "marry"), (3, "john"))
        val df = spark.createDataFrame(data).toDF("id", "name")
        df.createOrReplaceTempView("tmp1")
        sql("insert into badou.sparkhive1 select id, name from tmp1")
        sql("select * from badou.sparkhive1").show
    */

  }
}
