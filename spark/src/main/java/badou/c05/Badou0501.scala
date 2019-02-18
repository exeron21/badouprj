package badou.c05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Badou0501 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("spark://master:7077")
      .appName("badou0501")
      .enableHiveSupport()
      .getOrCreate()
    // 1.统计product被购买的次数
    // 2。统计Product被reordered的次数
    // 3。结合上面数量统计Product购买的Reordered比率

    val prior = spark.sql("select * from badou.order_prior")
    val tmp1 = prior.select("product_id")
    tmp1.groupBy("product_id").count().show(100)

    val tmp2 = prior.select("product_id", "reordered")
    tmp2.selectExpr("product_id", "cast(reordered as int)") // 这里cast完了之后应该1和0的值
      .groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_prod"), // 因为reordered在cast完之后是1和0,所以sum一下，其实就是reordered=1的数量
        avg("reordered").as("prod_cnt_prod"), // 同上，因为全是1和0，所以avg就是为1的比例
        count("product_id").as("prod_cnt"))  //  按product_id分组之后，只要不是空值 ，count什么都行。。 count(*),count(1),count(100),count(product_id)都是一样的结果

    // 统计用户特征
    // 1。用户平均购买订单的间隔周期
    // 2。用户的总订单量
    // 3。用户购买的Product商品去重之后的集合数据
    // 4。用户总商品数量以及去重后的商品列表
    // 5。用户购买的平均每个订单的商品数量
//    days_since_prior_order
    val order_tmp = spark.sql("select * from badou.orders")
    val order = order_tmp.selectExpr("*", "if (days_since_prior_order=='',0, days_since_prior_order) as dspo")
      .drop("days_since_prior_order")
    order
//      .where("user_id=1")
//      .where("order_number<=2")
      .groupBy("user_id")
      .agg(avg("dspo"))
      .show()

    order.groupBy("user_id").count().show
    import spark.implicits._
//    import order.sparkSession.implicits._ // 和上面一句话一样
    val op = order.join(prior, "order_id").select("user_id","product_id")
    val result = op.rdd.map(x=>(x(0).toString , x(1).toString)).groupByKey().mapValues(_.toSet.mkString(",")).toDF("user_id","prods")

    op.groupBy("user_id").agg(count("product_id"), countDistinct("product_id")).show()

    op.rdd.map(x=>
      (x(0).toString,x(1).toString)
    ).groupByKey()
      .mapValues{
        r=> val rs = r.toSet
          (rs.size, rs.mkString(","))
      }.toDF("user_id","tup")
      .selectExpr("user_id", "tup._1 as a1", "tup._2 as b2")

    val op2 = order.join(prior, "order_id").select("user_id","order_id","product_id")
    op2.groupBy("user_id", "order_id").count().show
    op2.persist()
    op2.cache()
    op2.rdd.map(x=>(x(0).toString, x(1).toString)).reduceByKey(_+_)
  }
}
