package features.transformer

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object StringIndexerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("StringIndexerDemo")
      .config("spark.sql.warehouse.dir", "e:\\badouprj\\spark-warehouse")
      .getOrCreate()

    val data = spark.createDataFrame(Seq(
      (0, "log"),
      (1, "text"),
      (2, "text"),
      (3, "soyo"),
      (4, "text"),
      (5, "log"),
      (6, "log"),
      (7, "log")
    )).toDF("id", "name")

    data.show()
    println("------------")
    val inde = new StringIndexer().setInputCol("name").setOutputCol("index_name")
    println(inde)
    println("------------")
    val fitDF = inde.fit(data)
    fitDF.labels.foreach(println)
    println("------------")
    val transForm = fitDF.transform(data)
    transForm.show()

    val vector = new VectorAssembler().setInputCols(Array("index_name")).setOutputCol("features")
//    val vector = new VectorAssembler().setInputCols(Array("id", "name")).setOutputCol("features")
    val dataDF = vector.transform(transForm)
    println("------------")
    dataDF.show()
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = inde.fit(data).transform(data)
    indexed.show()
  }
}
