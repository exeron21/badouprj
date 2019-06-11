package lr

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import util.SimpleFeature2

object LRTest2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("LRTest2")
      .enableHiveSupport()
      .getOrCreate()

    val orders = spark.sql("badou.orders")
    val priors = spark.sql("badou.priors")
    val trains = spark.sql("badou.trains")

    val trainOrders = orders.filter("eval_set='train'")
    val priorOrders = orders.filter("eval_set='prior'")
    val testOrders = orders.filter("eval_set='test'")
    trainOrders.count()
    priorOrders.count()
    testOrders.count()
    trainOrders.join(priors, "order_id").count()
    priorOrders.join(priors, "order_id").count() // 32434489
    testOrders.join(priors, "order_id").count()

    trainOrders.join(trains, "order_id").count() // 1384617
    priorOrders.join(trains, "order_id").count()
    testOrders.join(trains, "order_id").count()

    val (prodFeat, userFeat) = SimpleFeature2.features(orders, priors)

    // 131209
    val trainUser = orders.filter("eval_set='train'").select("user_id").distinct()
    // 75000
    val testUser = orders.filter("eval_set='test'").select("user_id").distinct()
    // 206209
    val priorUser = orders.filter("eval_set='prior'").select("user_id").distinct()

    // 0 test和train是没有交集的
    val testTrain = testUser.intersect(trainUser)
    // 75000 test和prior
    val testPrior = testUser.intersect(priorUser)
    // 131209 train和prior
    val trainPrior = trainUser.intersect(priorUser)
//    testTrain.count()
//    testPrior.count()
//    trainPrior.count()

    val orderPrior = orders.join(priors, "order_id")
    val orderTrain = orders.join(trains, "order_id")

    val userRecall = orderPrior.select("user_id", "product_id").distinct()

    val userReal = orderTrain.select("user_id", "product_id").distinct()
      .withColumn("label", lit(1))

    val trainData = userRecall.join(userReal, Seq("user_id", "product_id"), "outer")
      .na.fill(0)
    val train = trainData.join(userFeat, "user_id")
      .join(prodFeat, "user_id")

    // 模型计算
    val rFormula = new RFormula().setFormula("label ~ u_avg_day_gap")
      .setFeaturesCol("features")
      .setLabelCol("label")
    val df = rFormula.fit(train).transform(train).select("features", "label")

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val lrModel = lr.fit(trainingData)
    println(s"Coefficient: ${lrModel.coefficients} intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory

    objectHistory.foreach(println)

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)

    val test = lrModel.transform(testData)
  }

}
