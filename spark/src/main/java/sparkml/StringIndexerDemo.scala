package sparkml
<<<<<<< HEAD

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
=======
import org.apache.spark.ml.feature.StringIndexer
>>>>>>> origin/master
import org.apache.spark.sql.SparkSession

object StringIndexerDemo {
  def main(args: Array[String]): Unit = {
<<<<<<< HEAD
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
    val indexer = new StringIndexer().setInputCol("name").setOutputCol("index_name")
    println(indexer)
    println("------------")
    val fitDF = indexer.fit(data)
    fitDF.labels.foreach(println)
    println("------------")
    val transForm = fitDF.transform(data)
    transForm.show()

    val vector = new VectorAssembler().setInputCols(Array("id", "name")).setOutputCol("features")
    val dataDF = vector.transform(transForm)
    println("------------")
    dataDF.show()
  }
=======
    val spark = SparkSession
      .builder().appName("HashingTFDemo")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:/data/spark-warehouse")
      .getOrCreate()

    val df = spark.createDataFrame(
      Seq(
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
      )
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
  }

>>>>>>> origin/master
}
