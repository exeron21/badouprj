package sparkml.lr

import badou.c05.SimpleFeature
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object LRTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("LR")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    val orders = spark.sql("select * from badou.orders")
    val priors = spark.sql("select * from badou.priors")
    val trains = spark.sql("select * from badou.trains")

    val (userFeat, prodFeat)= SimpleFeature.feature(priors, orders)
    /**
      * scala> trainUser.count()
      * res9: Long = 131209
      */
    val trainUser = orders.filter("eval_set='train'").select("user_id").distinct()
    /**
      * scala> testUser.count
      * res10: Long = 75000
      */
    val testUser = orders.filter("eval_set='test'").select("user_id").distinct()
    /**
      * scala> priorUser.count
      * res12: Long = 206209
      */
    val priorUser = orders.filter("eval_set='prior'").select("user_id").distinct()

    /**
      * scala> interset.count()
      * res11: Long = 0
      */
    var interset = trainUser.intersect(testUser)
    interset.count()
    /**
      * scala> interset.count
      * res13: Long = 131209
      */
    interset = priorUser.intersect(trainUser)
    interset.count()
    /**
      * scala> interset.count
      * res14: Long = 75000
      */
    interset = priorUser.intersect(testUser)
    interset.count()

    // 结论： priorUser(206209) = trainUser(131209) + testUser(75000)

    val op = orders.join(priors, "order_id") // eval_set 全是prior, 条数32434489
    val optrain = orders.join(trains, "order_id") // eval_set 全是test, 条数1384617

    /**
      * scala> user_recall.count
      * res34: Long = 13307953
      */
    val user_recall = op.select("user_id", "product_id").distinct()
      .withColumn("label", lit(0))
    /**
      * scala> user_real.count
      * res35: Long = 1384617
      */
    val user_real = optrain.select("user_id", "product_id").distinct()
      .withColumn("label", lit(1))

    // 828824
    val intersect1 = user_recall.select("user_id","product_id").intersect(user_real.select("user_id","product_id"))
    // 正样本、负样本：1384617, 12479129
    val train_data = user_recall.join(user_real, Seq("user_id", "product_id"), "outer").na.fill(0)

    val train = train_data.join(userFeat, "user_id").join(prodFeat, "product_id")

    val rformula = new RFormula()
      .setFormula("label ~ u_avg_da_gap")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val df = rformula.fit(train)
      .transform(train)
      .select("features", "label")

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    val lrModel = lr.fit(trainingData)

    print(s"Coefficients: ${lrModel.coefficients} , intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory

    objectHistory foreach println

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val roc = binarySummary.roc

    roc.show()

    println(binarySummary.areaUnderROC)

    val test = lrModel.transform(testData)
  }
}
