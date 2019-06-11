package features

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * SparkMl One-hot, Pipeline, 训练测试与评估
  */
object FeatureTest {

  def getDataFromJsonFile(spark: SparkSession, path: String):DataFrame = {
    spark.read.option("inferSchema", "true")
    .json(path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("FeatureTest01")
      .config("spark.sql.warehouse.dir", "e:\\badouprj\\spark-warehouse")
      .getOrCreate()

//    val path = "file:///E:\\data\\ml-100k\\u.data"
    val path = "file:///E:\\data\\lr_test03.json"
    println(s"从json文件中读取数据, 文件路径: = $path")
    val data = getDataFromJsonFile(spark, path)
//    val data = getDataFromCsvFile(spark, path)
    data.printSchema()
    data.limit(10).show()


    println("采用Pileline方式处理机器学习流程:")
    val categoricalColumns = Array("gender", "children")
    /** 采用Pileline方式处理机器学习流程 */
    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- categoricalColumns) {
      /** 使用StringIndexer 建立类别索引 */
      val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index").setHandleInvalid("skip").fit(data)
      println(indexer.explainParams())
      /** 使用OneHotEncoder将分类变量转换为二进制稀疏向量 */
      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec")
      stagesArray.append(indexer, encoder)
    }

    System.exit(0)

    ////////////////////////////

    val numericCols = Array("affairs", "age", "yearsmarried", "religiousness", "education", "occupation", "rating")
    val assemblerInputs = categoricalColumns.map(_ + "classVec") ++ numericCols
    /**使用VectorAssembler将所有特征转换为一个向量*/
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stagesArray.append(assembler)

    /////////////////////
    println("开始pipeline转换：")
    val pipeline = new Pipeline()
    println("stageArray: ")
    stagesArray.foreach(println)
    pipeline.setStages(stagesArray.toArray)
    /**fit() 根据需要计算特征统计信息*/
    val pipelineModel = pipeline.fit(data)
    /**transform() 真实转换特征*/
    val dataset = pipelineModel.transform(data)
    dataset.show(false)

    ///////////////////
    println("分割数据集")
    /**随机分割测试集和训练集数据，指定seed可以固定数据分配*/
    val Array(trainingDF, testDF) = dataset.randomSplit(Array(0.6, 0.4), seed = 12345)
    println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
    println("生成lr模型")
    val lrModel = new LogisticRegression()
      .setLabelCol("affairs")
      .setFeaturesCol("features")
      .fit(trainingDF)
    import spark.implicits._
    val predictions = lrModel.transform(testDF).select($"affairs".as("label"), $"features", $"rawPrediction", $"probability", $"prediction")
    println("模型预测结果：")
    predictions.show(200,truncate = false)
    println(s"lrModel.coefficients: ${lrModel.coefficients}, intercept: ${lrModel.intercept}")
    /**使用BinaryClassificationEvaluator来评价我们的模型。在metricName参数中设置度量。*/
    val evaluator = new BinaryClassificationEvaluator()
    evaluator.setMetricName("areaUnderROC")
    val auc= evaluator.evaluate(predictions)
    println(s"areaUnderROC=$auc")

    val lrSummary = lrModel.summary
    val objectHistory = lrSummary.objectiveHistory
    println("objectHistory: ")
    for (elem <- objectHistory) {
      println(elem)
    }

    val trainingSummary = lrSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    val trainingSummaryRoc = trainingSummary.roc
    trainingSummaryRoc.show(200)
    val auc2 = trainingSummary.areaUnderROC
    println(s"trainingSummary.areaUnderROC: $auc2")
  }


  def getDataFromCsvFile(spark: SparkSession, str: String): DataFrame = {
    spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("sep", "\t").csv(path = str).toDF("userId", "itemId", "rating", "timestamp")
  }

}
