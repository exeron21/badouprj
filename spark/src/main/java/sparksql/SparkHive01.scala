package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object SparkHive01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .master("local")
        .appName("sparkHive")
//        .config("spark.sql.warehouse.dir", "/usr/hive/warehouse")
//        .enableHiveSupport()
        .getOrCreate()
    val sc = spark.sparkContext
    val emp = sc.textFile("file://E:\\bigdata\\emp.csv")
    val st = new StructType(Array(
      StructField("id", IntegerType, true),
      StructField("id", IntegerType, true),
      StructField("id", IntegerType, true),
      StructField("id", IntegerType, true),
      StructField("id", IntegerType, true),
      StructField("id", IntegerType, true)
    ))
//    val df = spark.sql("show databases")
//    df.show()

/*    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val url = "jdbc:hive2://master:10000"
    val user = ""
    val pwd = ""
    val conn = DriverManager.getConnection(url)
    val stst = conn.createStatement()
    val rs = stst.executeQuery("show databases")
    while(rs.next()) {
      println(rs.getString(1))
    }*/
  }
}
