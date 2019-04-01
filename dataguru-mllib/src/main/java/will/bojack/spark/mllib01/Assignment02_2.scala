package will.bojack.spark.mllib01

import org.apache.spark.sql.SparkSession
import will.bojack.spark.mllib01.Assignment02.Package


object Assignment02_2 {
  /**
    * 作业第2题,用SparkSQL实现
    */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Assignment02")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("select indate, userid, appid from exam.inapp")
    val count = df.count() //
    println(count)

    val userCount = df.select("userid").distinct().count()
    println(userCount)

    val dateCount = df.select("indate").distinct().count()
    println(dateCount)

    val twoDate = df.filter("indate in('2016-04-01','2016-04-02')") // 744752
    val date = ("2016-04-01", "2016-04-02")

    val result1 = twoDate.rdd.map(x=>{
      (x(1), Package(x(0).toString, x(1).toString, x(2).toString))
    }).groupByKey().mapValues(y=>{
      val r1 = y.filter(z=> z.date.equals(date._1));
      val r2 = y.filter(z=> z.date.equals(date._2));
      r2.toSet diff r1.toSet
    }).filter(_._2.nonEmpty)

    val result2 = result1.mapValues(x=>{x.toArray.length})  // 9554
  }
}
