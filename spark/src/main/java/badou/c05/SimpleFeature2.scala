package badou.c05

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object SimpleFeature2 {
  def main(args: Array[String]): Unit = {

  }

  def features(orders:DataFrame, priors:DataFrame):(DataFrame, DataFrame) = {
    /** priors表：订单-商品数据
      order_id: integer
      product_id: integer
      add_to_cart_order: integer
      reordered: integer
      * 第五次课作业, Product Feature
      * 1、统计Product被购买的数据量 prod_ord_cnt
      * 2、统计Product被reordered的数量（再次购买） prod_reorr_cnt
      * 3、结合上面数量统计Product购买的Reordered的比率  prod_reord_rate
      */
    val prodFeat = priors.groupBy("product_id")
      .agg(count("reordered").as("prod_ord_cnt"),
        sum("reordered").as("prod_reord_cnt"),
        avg("reordered").as("prod_reord_rate"))
    /**
      * orders表：订单数据
      *
      order_id: integer
      user_id: integer
      eval_set: string
      order_number: integer
      order_dow: integer
      order_hour_of_day: integer
      days_since_prior_order: string

      * user Features
      * 1、每个用户购买订单的平均间隔 userOrdGap
      * 2、每个用户的总订单数 userOrdCnt
      * 3、每个用户购买的Product商品去重后的集合数据,需要priors数据
      * 4、每个用户总商品数量以及去重后商品数量,需要priors数据
      * 5、每个用户购买的平均每个订单商品数量
      */
    val userOrdGap = orders.selectExpr("*", "if (days_since_prior_order ='',0, days_since_prior_order) as dspo")
      .drop("days_since_prior_order")
      .selectExpr("user_id", "order_id", "cast(dspo as double) as dspo")
      .groupBy("user_id")
      .avg("dspo")

    val userOrdCnt = orders.selectExpr("user_id", "order_id")
      .groupBy("user_id")
      .count()

    val op = orders.join(priors, "order_id").select("user_id", "product_id")
    import orders.sparkSession.implicits._
    // 第3个特征
    op.rdd.map(x=>{
      (x(0).toString, x(1).toString)
    }).groupByKey().mapValues({
      _.toSet.mkString("_")
    }).toDF("user_id", "prod_list")

    // 第3和4个特征
    val userFeat34 = op.rdd.map{
      x=>(x(0).toString, x(1).toString)
    }.groupByKey().mapValues{x=>
      val set = x.toSet
      (set.size, set.mkString("_"))
    }.toDF("user_id", "tup").selectExpr("user_id", "tup._1 as size", "tup._2 as list")

    val userFeat = userFeat34
    (userFeat, prodFeat)
  }
}
