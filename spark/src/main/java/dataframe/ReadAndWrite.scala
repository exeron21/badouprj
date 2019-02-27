package dataframe

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.beans.BeanProperty

object ReadAndWrite {
  case class Person(@BeanProperty id:Int, @BeanProperty name:String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("writeToHive")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    println("sc.version = " + sc.version)
    sc.setLogLevel("WARN")
//    val df = readFromHdfsCsv(spark, "/data/person.txt")
    val df = readFromDb(spark)
    print(df.na)
    df.printSchema()
    df.show()
  }

  def readFromHdfsCsv(spark:SparkSession, path:String) : DataFrame = {
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load(path)
  }

  def readFromHive(spark:SparkSession) :DataFrame = {
    spark.sql("select * from badou.person")
  }

  def writeToHive(df:DataFrame):Unit = {
    df.write.insertInto("badou.person")
  }

  def readFromDb(spark:SparkSession): DataFrame = {
    val url = "jdbc:mysql://master:3306/test"
    val table = "person"
    val user = "root"
    val password = "Willingly0510"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    spark.read.jdbc(url, table, prop)
  }

  def writeToDb(df:DataFrame):Unit ={
    val url = "jdbc:mysql://master:3306/test"
    val table = "person"
    val user = "root"
    val password = "Willingly0510"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    df.write.mode(SaveMode.Append).jdbc(url, table, prop)
  }
}
