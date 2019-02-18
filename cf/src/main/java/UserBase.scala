import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UserBase")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val udata = spark.sql("select * from badou.udata")

    // 1.计算相似用户 cosine = a*b/(|a|*|b|)
    //
    import spark.implicits._
    val userScoreSum = udata.rdd.map(x=>(x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x=>
        math.sqrt(x.toArray.map(rating=>math.pow(rating.toDouble, 2)).sum)
      ).toDF("user_id", "rating_sqrt_sum")

    // 纯dataframe写法：
    // sum()可以写成sum("pow_rating")
    // 如果用agg的话: agg("pow_rating" -> "sum") 前面是需要聚合的列，后面是聚合函数，结果字段名变成 聚合函数(聚合列)，如sum(pow_rating)
//    val userScoreSum = udata.selectExpr("user_id", "pow(rating, 2) as pow_rating")
//      .groupBy("user_id")
//      .sum()
//      .withColumnRenamed("sum(pow_rating)", "sum_pow_rating")
//      .selectExpr("user_id", "sqrt(sum_pow_rating) as sqrt_sum_pow_rating")

    // 1.1 item->user倒排表
    val df = udata.selectExpr("user_id as user_v", "item_id as item_v", "rating as rating_v")

// 如果join的两个字段名称不一样，则用下面的几种方式：
// 这几种方式可以解决join字段名称不一样的问题，但是结果中会同时包含这两个字段,哪怕这两个字段的值完全一样
// 所以还是把两个字段名称设置成一样更为方便
//    val df_join0 = udata.join(df, udata("item_id") === df("item_v"))
//    val df_join1 = udata.join(df, $"item_id" === $"item_v")
//    val df_join2 = udata.join(df).where($"item_id" === $"item_v")
    // df_join 是用户与用户之间两两对应，并过滤了相同的值
    val df_join = udata.join(df, "item_id").filter("cast(user_id as long) <> cast(user_v as long)")

    import org.apache.spark.sql.functions._
    val product_udf = udf((s1:Int, s2:Int)=>s1.toDouble * s2.toDouble)

    // 将df_join(item相同的记录对应的)rating乘到一起(两个向量的每个维度的乘积)
    val df_product = df_join.withColumn("rating_product", product_udf(col("rating"),col("rating_v")))
      .select("user_id", "user_v", "rating_product")

    // 计算向量a与向量b的点乘（向量a的主体是用户1,向量中的每个元素是用户1对每部电影的打分，b同理）
    // 实现方式是对两个用户做分组聚合（就是将每个维度的乘积加到一起，形成点乘）
    // 使用了agg函数之后列名会变，下例中变成了"sum(rating_product)"，要用withColumnRenamed函数改名
    val df_sim_group = df_product.groupBy("user_id", "user_v")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)","rating_sum_pro")

    val userScoreSumV = userScoreSum.selectExpr("user_id as user_v", "rating_sqrt_sum as rating_sqrt_sum_v")

    val dfResult = df_sim_group.join(userScoreSum, "user_id")
      .join(userScoreSumV, "user_v")
      .selectExpr("user_id", "user_v", "rating_sum_pro/ (rating_sqrt_sum * rating_sqrt_sum_v) as cosine")
    dfResult.show(10)

    dfResult.rdd.map(x=>(x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey().mapValues{x=>
      x.toArray.sortWith((x,y)=>x._2>y._2).slice(0,10)
    }.flatMapValues(x=>x).toDF("user_id", "user_v_sim")
      .selectExpr("user_id", "user_v_sim._1 as user_v","user_v_sim._2 as sim")
  }

  val filterUDF = udf{(items:Seq[String], items_v:Seq[String]) =>
    val fMap = items.map{x=>
      val l = x.split("_")
      (l(0), l(1))
    }.toMap
    items_v.filter{x=>
      val l = x.split("_")
      fMap.getOrElse(l(0), -1) == -1
    }
  }


  val ratSimUdf = udf{(sim:Double, ratings:Seq[String])=>
    ratings.map{x=>
      val l = x.split("_")
      l(0) + "_" + l(1).toDouble*sim
    }
  }
}
