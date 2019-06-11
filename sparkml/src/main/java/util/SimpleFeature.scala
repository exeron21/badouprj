package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, count, sum}

object SimpleFeature {
  def main(args: Array[String]): Unit = {

  }

  def feature(priors:DataFrame, orders:DataFrame):(DataFrame,DataFrame) = {
    /**
      * 第五次课作业, Product Feature
      * 1、统计Product被购买的数据量 prod_ord_sum
      * 2、统计Product被reordered的数量（再次购买） prod_reorder_sum
      * 3、结合上面数量统计Product购买的Reordered的比率  prod_reorder_rate
      */
    val prodFeat = priors.selectExpr("product_id", "cast(reordered as int)")
      .groupBy("product_id")
     // agg接收多个聚合函数做为参数
      /**
        * 因为reordered的值为1和0，1表示本订单中本产品是重新购买的，0不是重新购买，没有其他情况，
        * 因此groupBy("product_id")之后的count("reordered")就是product所有的购买量，
        * sum("reordered")就是product的reordered量，avg就相当于sum("reordered")/count("reordered")
        */
      .agg(sum("reordered").as("prod_sum_rod"),
        avg("reordered").as("prod_rod_rate"),
        count("product_id").as("prod_reorder_cnt")) // 这里不能count("1")，会报错



    /**
      * orders表：订单数据
      * priors表：订单-商品数据
      * 1、每个用户购买订单的平均间隔
      * 2、每个用户的总订单数
      * 3、每个用户购买的Product商品去重后的集合数据,需要priors数据
      * 4、每个用户总商品数量以及去重后商品数量,需要priors数据
      * 5、每个用户购买的平均每个订单商品数量
      */

    /**
      * "if (days_since_prior_order==\'\',0,days_since_prior_order) as dspo")
      * 上条语句中，days_since_prior_order是String类型的，但仍然可以if后面指定为数字型的0
      * if 和 cast 是udf，所以后面要加小括号
      * if 之后的生成的字段名字会乱七八糟的，cast之后不会
      * 例子：select if (order_number%2==0,0,1),cast(days_since_prior_order as int) from orders limit 20;
      * 这条语句第一个字段会变成_c0，后面的字段不变
      */
      //1: days_since_prior_order距离上次购买的日期间隔，为空的要处理为0，然后groupBy("user_id")之后avg一下就行了
    val userOrderGap = orders.selectExpr("*", "if (days_since_prior_order=='',0,days_since_prior_order) as dspo")
        .drop("days_since_prior_order")
        .selectExpr("user_id","cast(dspo as int) as dspo")
        .groupBy("user_id").avg("dspo")

    //2:
    val userOrderCnt = orders.groupBy("user_id").count()
    //3:
    val join = orders.join(priors, "order_id").select ("user_id","product_id")
    import orders.sparkSession.implicits._
    join.rdd.map{
      x=>(x(0).toString,x(1).toString)
    }.groupByKey()
      .mapValues(_.toSet.mkString(","))
      .toDF("user_id","prod_dis_cnt")
    //4
    val userProRcdSize = join.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey().map{x=>
      val set = x._2.toSet
        (set.size,set.mkString("==")) }
      .toDF("user_id","tup")
      .selectExpr("user_id","tup._1 as prod_cnt", "tup._2 as prod_str")

    val userProRcdSize2 = join.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey().mapValues{r =>
      val rs = r.toSet
      (rs.size, rs.mkString(","))
    }.toDF("user_id","tuple")
      .selectExpr("user_id","tuple._1 as prod_cnt", "tuple._2 as prod_str")
    //上面这两种结果完全不同,看看PairRDDr的mapValue函数：
    // def mapValues[U](f: (V) ⇒ U): RDD[(K, U)]
    //Pass each value in the key-value pair RDD through a map function without changing the keys;
    // this also retains the original RDD's partitioning.p
    (userProRcdSize, prodFeat)
  }

}
