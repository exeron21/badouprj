package features.transformer

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

object OneHotStringIndexerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("HashingTFDemo")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:/data/spark-warehouse")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    indexed.show()
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
//      .setDropLast(false)
    val encoded = encoder.transform(indexed)
    encoded.show()
  }
}
