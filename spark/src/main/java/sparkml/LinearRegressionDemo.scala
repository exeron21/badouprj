package sparkml

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
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
    val data = spark.read.option("inferSchema", "true")
      .option("header", "true")
      .csv(path)
    data.show(100)
    val stringIndexer = new StringIndexer()
      .setInputCol("col")
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCol("features")
      .setOutputCol("col1")
      .setDropLast(false)

    val lr = new LinearRegression().setMaxIter(10)
      .setRegParam(0.01)
    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2))
    val lrModel = lr.fit(trainData)
    val result = lrModel.transform(testData)
    result.show()
  }
}
