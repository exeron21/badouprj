package dataframe

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.beans.BeanProperty

object ReadAndWrite {
  case class Person(@BeanProperty id:Int, @BeanProperty name:String)

  def test(spark:SparkSession, path:String):DataFrame= {
    val df = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "true") // inferSchema .. 自动判断数据类型... 默认是false, 如果是false,读文件生成的字段都是字符型的.
      .csv("file:///E:\\data\\rating")
    df.write.mode(SaveMode.Overwrite).json("file:///E:\\data\\rating.json")
    return df
  }
  def readFromJson(spark: SparkSession, path: String) = {
    spark.read.json(path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("writeToHive")
//      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "file:///e:/data/")
      .getOrCreate()
    val sc = spark.sparkContext
    println("sc.version = " + sc.version)
    sc.setLogLevel("WARN")
//    val df = readFromJson(spark, "/data/person.txt")
//    val df = readFromDb(spark)

//    val df = test(spark, null)

    val df = readFromJson(spark, "E:\\data\\rating1.json")

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
