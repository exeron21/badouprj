package sparkml.features.transformer

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

object OneHotEncoderDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:/data/spark-warehouse")
      .appName("OneHotEncoder")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, "log"),
      (1, "text"),
      (2, "text"),
      (3, "soyo"),
      (4, "text"),
      (5, "log"),
      (6, "log"),
      (7, "log"),
      (8, "hadoop")
    )).toDF("id", "label")
    val df2 = spark.createDataFrame(Seq(
      (0, "log"),
      (1, "soyo"),
      (2, "soyo")
    )).toDF("id", "label")
    val indexer = new StringIndexer().setInputCol("label").setOutputCol("label_index")
    val indexerModel = indexer.fit(df)
    val indexed1 = indexerModel.transform(df) //这里测试数据用的是df
    indexed1.show()
    // setDropLast：被编码为全0向量的标签也可以占有一个二进制特征
    val encoder = new OneHotEncoder().setInputCol("label_index").setOutputCol("lable_vector").setDropLast(false)
    val encodered1 = encoder.transform(indexed1)
    encodered1.show()

    //(4,[2],[1.0]) //这里的4表示训练数据中有4中类型的标签
    //测试数据换为df2
    val indexModel2 = indexer.fit(df2)
    val indexed = indexModel2.transform(df2)
    val encodered = encoder.transform(indexed)
    encodered.show()
  }
}
