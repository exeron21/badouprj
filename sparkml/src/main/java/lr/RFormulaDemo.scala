package lr

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.{DataFrame, SparkSession}

object RFormulaDemo {

  def createDataFrameFromHardCode(spark:SparkSession): DataFrame = {
    spark.createDataFrame(Seq(
      (1, 11, "US", 18, 1.0),
      (2, 12, "US", 18, 1.0),
      (3, 13, "US", 18, 1.0),
      (4, 14, "US", 18, 1.0),
      (5, 15, "US", 18, 1.0),
      (6, 16, "US", 18, 1.0),
      (7, 17, "US", 18, 1.0),
      (8, 22, "CA", 12, 0.0),
      (9, 40, "CA", 15, 0.0),
      (10, 29, "CA", 15, 1.0),
      (11, 88, "CA", 15, 0.0),
      (12, 44, "DA", 5, 1.0),
      (13, 99, "CA", 2, 0.0)))
      .toDF("id", "count", "country", "hour", "clicked")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(" ")
      .config("spark.sql.warehouse.dir", "e:/badouprj/spark-warehouse")
      .master("local")
      .getOrCreate()
    val dataset = createDataFrameFromHardCode(spark)
    /**
      * country列6个不同取值时候占了五个维度  五个不同取值时候占了四个维度
      * 四个不同取值时候占了三个维度  三个不同取值占了两维度  两个不同取值占了一个维度
      * 另外我们还操作了非StringType类型的hour和count列
      * 因此在country列所占维度基础上 再加上两个维度，就是所形成的新列features
      * 该列值是一个向量  由上面组成的维度构成
      */
    val formula = new RFormula().setFormula("clicked ~ country").setFeaturesCol("features").setLabelCol("label")
    val output = formula.fit(dataset).transform(dataset)
    output.show()
    System.exit(0)

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)
    val Array(trainingData, testData) = output.randomSplit(Array(0.8, 0.2))
    val lrModel = lr.fit(trainingData)
    println(s"coefficients: ${lrModel.coefficients} , intercept: ${lrModel.intercept}")
    val trainingSummary = lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory
    objectHistory foreach println
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)
    val test = lrModel.transform(testData)
    trainingData.show(100)
    test.show(100)

    // output.write.json("spark-warehouse/Rformula")
    // output.select("features", "label").show()
  }
}
