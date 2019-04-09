package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

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

/**
  * +---+--------+-------------+
  * | id|category|categoryIndex|
  * +---+--------+-------------+
  * |  0|       a|          0.0|
  * |  1|       b|          2.0|
  * |  2|       c|          1.0|
  * |  3|       a|          0.0|
  * |  4|       a|          0.0|
  * |  5|       c|          1.0|
  * +---+--------+-------------+
  *
  * +---+--------+-------------+-------------+
  * | id|category|categoryIndex|  categoryVec|
  * +---+--------+-------------+-------------+
  * |  0|       a|          0.0|(2,[0],[1.0])|
  * |  1|       b|          2.0|    (2,[],[])|
  * |  2|       c|          1.0|(2,[1],[1.0])|
  * |  3|       a|          0.0|(2,[0],[1.0])|
  * |  4|       a|          0.0|(2,[0],[1.0])|
  * |  5|       c|          1.0|(2,[1],[1.0])|
  * +---+--------+-------------+-------------+
  *
  * +---+--------+-------------+
  * | id|category|categoryIndex|
  * +---+--------+-------------+
  * |  0|       a|          0.0|
  * |  1|       b|          2.0|
  * |  2|       c|          1.0|
  * |  3|       a|          0.0|
  * |  4|       a|          0.0|
  * |  5|       c|          1.0|
  * +---+--------+-------------+
  * 稀疏向量则可以根据下表表示,(3,[4,5,6],[1,2,3])，第一个值代表大小，第二个代表下标数组，第二个是下标对应的值。
  * +---+--------+-------------+-------------+
  * | id|category|categoryIndex|  categoryVec|
  * +---+--------+-------------+-------------+
  * |  0|       a|          0.0|(3,[0],[1.0])|
  * |  1|       b|          2.0|(3,[2],[1.0])|
  * |  2|       c|          1.0|(3,[1],[1.0])|
  * |  3|       a|          0.0|(3,[0],[1.0])|
  * |  4|       a|          0.0|(3,[0],[1.0])|
  * |  5|       c|          1.0|(3,[1],[1.0])|
  * +---+--------+-------------+-------------+
  */