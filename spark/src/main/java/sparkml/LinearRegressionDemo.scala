package sparkml

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegressionDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("HashingTFDemo")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:/data/spark-warehouse")
      .getOrCreate()

    val path = "file:///e:/data/people.csv"
    val data = spark.read.option("inferSchema", "true").option("sep", ",").csv(path)
      .toDF("height", "gender", "weight")

    data.show(30)

    val rFormula = new RFormula()
      .setFormula("weight ~ height")
      .setFeaturesCol("features")
      .setLabelCol("label")
/*    val stringIndexer = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("idx_gender")
      .setHandleInvalid("skip")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCol("idx_gender")
      .setOutputCol("onehot_gender")
      .setDropLast(false)

    val vectorAssembler = new VectorAssembler().setInputCols(Array("height", "onehot_gender"))
      .setOutputCol("features")*/

//    val stages = Array(stringIndexer, oneHotEncoder, vectorAssembler)
//    val data1= new Pipeline().setStages(stages).fit(data).transform(data)
//    data1.show()
    val data1 = rFormula.fit(data).transform(data).select("features", "label")

    data1.show(30)
    val lr = new LinearRegression().setMaxIter(10)
      .setRegParam(0)
    val Array(trainData, testData) = data1.randomSplit(Array(0.8, 0.2))
    val lrModel = lr.fit(trainData)
    val result = lrModel.transform(testData)
    result.show()
  }
}
