import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserBase2 {

  /**
    *
    * 两个向量的余弦相似度= (a*b)/||a||*||b||
    * 分子a1*b1 + a2*b2 +...
    * 分母sqrt(a1^2+a2^2+...+an^2) * sqrt(b1^2+b2^2+...+bn^2)
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("userBase")
      .getOrCreate()

    val udata = spark.sql("select * from badou.udata")
    val udata2 = udata.selectExpr("user_id as user_id2", "item_id", "rating as rating2")
    val dfjoin = udata.join(udata2, "item_id").selectExpr("user_id", "user_id2", "item_id", "rating * rating2 as rating").filter("user_id <> user_id2")
    // 注意调用udf时要用withColumn，不能用selectExpr
    //    val udf_multiple = udf((x:Int, y:Int)=>x*y)
    //    val dfjoin2 = udata.join(udata2, "item_id").withColumn("rating" ,udf_multiple(col("rating"), col("rating2")))
    //        .select("user_id", "user_id2", "item_id", "rating")
    val dfjoinsum = dfjoin.groupBy("user_id", "user_id2").sum("rating").withColumnRenamed("sum(rating)", "rating1")
//    val dfjoinsum2 = dfjoin.groupBy("user_id", "user_id2").agg("rating"->"sum").withColumnRenamed("sum(rating)", "rating")

    import spark.implicits._
    val dfm = udata.rdd.map(x=>{
      (x(0).toString, x(2).toString)
    }).groupByKey().mapValues(y=>{
      y.toArray.map(z=>{
        math.pow(z.toDouble,2)
      }).sum
    }).toDF("user_id", "rating_sum")
    val dfm2 = dfm.selectExpr("user_id", "sqrt(rating_sum) as rating2")
    val dfm3 = dfm2.selectExpr("user_id as user_id2", "rating2 as rating3")

    val dfSim = dfjoinsum.join(dfm2, "user_id").join(dfm3, "user_id2").selectExpr("user_id", "user_id2", "rating1/(rating2 * rating3)")

    dfSim.rdd.map(x=>(x(0).toString,(x(1).toString, x(2).toString)))
      .groupByKey().mapValues(x=>{
      x.toArray.sortWith((x,y)=>x._2>y._2).slice(0,10)
    }).flatMapValues(x=>x).toDF("")
  }
}
