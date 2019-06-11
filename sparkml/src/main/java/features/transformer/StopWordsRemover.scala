package features.transformer

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

object StopWordsRemover {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().appName("HashingTFDemo")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:/data/spark-warehouse")
      //      .enableHiveSupport()
      .getOrCreate()
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()
  }
}
