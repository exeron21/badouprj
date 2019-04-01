package will.bojack.spark.mllib01

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Assignment03 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Assignment03")
      .enableHiveSupport()
      .getOrCreate()

    val dateCount = spark.sql("select count(*) as count, indate from exam.inapp group by indate order by count desc")
    dateCount.show()

    /**
      * 0401 数据量443842
      * distinct 之后数据量443829
      * 同一天安装同样的包还是少见
      * @param spark
      */
    def inappByDate(spark:SparkSession, date:String) = {
      val date = "2016-04-17"
      val tmp = date.substring(5, 10).replace("-", "")
      val tabName = "inapp"+tmp
      val df= spark.sql(s"select * from exam.inapp where indate ='$date'")
      df.write.insertInto(s"exam.$tabName")

      val dfByDate = spark.sql(s"select * from exam.$tabName")

      val dfCount = dfByDate.count()

      val df0401Distinct = dfByDate.select("userid","appid").distinct()
      val dfDisCount = df0401Distinct.count()

      val df2 = df0401Distinct.selectExpr("userid", "appid as appid_v")
      val dfjoin = df0401Distinct.join(df2, "userid").groupBy("appid", "appid_v").agg("appid"->"count").withColumnRenamed("count(appid)", "count").filter("appid > appid_v").orderBy(desc("count"))
      val dfjoin2 = dfjoin.selectExpr("appid","appid_v", "count", s"count/$dfDisCount as cnt")
      dfjoin2.show
    }

    def joinTest(sc:SparkContext) = {
      import spark.implicits._
//      val r1 = Range(1, 10)
      val r1 = Array(1,1,1,1,1,1,1,1)
      val ran = new Random()
      val r2 = r1.map(x=>x * ran.nextInt() % 1000)
      val r3 = r1.zip(r2)
//      println(r3)
      val r4 = r1.map(x=>x * ran.nextInt() % 1000)
      val r5 = r1.zip(r4)
//      println(r5)
      val d1 = sc.parallelize(r3).toDF("id", "score")
      val d2 = sc.parallelize(r5).toDF("id", "score1")
      d1.join(d2,"id").count

      val dx1 = sc.parallelize(Range(0,10))
    }
    /**
      * 只分析com.picsart.studio|com.facebook.katana 这两个包的数据
      * 1. 将这两个包的数据插入inapp2表,
      * 总数据量 122668, 分别数据量
      * com.picsart.studio   13257
      * com.facebook.katana  109411
      *
      * 求两个物品同时被多少人买过:
      * +-------------------+-------------------+-----+
      * |              appid|            appid_v|count|
      * +-------------------+-------------------+-----+
      * | com.picsart.studio|com.facebook.katana| 4503|
      * |com.facebook.katana| com.picsart.studio| 4503|
      * |com.facebook.katana|com.facebook.katana|59763|
      * | com.picsart.studio| com.picsart.studio| 7376|
      * +-------------------+-------------------+-----+
      */
    def inapp2(spark:SparkSession) ={
      val df1 = spark.sql("select * from exam.inapp where appid in ('com.picsart.studio','com.facebook.katana')")
      df1.write.insertInto("exam.inapp2")

      val df2 = spark.sql("select * from exam.inapp2")
      val count1 = df2.count()
      val count2 = df2.groupBy("appid").count

      // 购买xxx商品的用户,
      val df3 = df1.groupBy("appid", "userid").count
      val df4 = df3.selectExpr("appid as appid_v", "userid", "count as cnt")
      df3.join(df4,"userid").groupBy("appid", "appid_v").sum().show


      val inapp2 = spark.sql("select userid,inapp from exam.inapp2 group by userid, inapp")

    }
  }
}
