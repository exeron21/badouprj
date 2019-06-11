package kmeans

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object kmeans_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kmeans_test")
      .config("spark.sql.warehouse.dir", "e:\\badouprj\\spark-warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

    def segment(df: DataFrame, column: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jiebaUdf = udf { sentence:String=> {
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
        }
      }
      df.withColumn("seg", jiebaUdf(col(column)))
    }

    val df = spark.sql("select split(sentence,' ') as sentence,label from badou.news_seg")
//    val df_ = segment(df, "")

    val tf = new HashingTF().setBinary(false).setInputCol("sentence").setOutputCol("rawFeatures").setNumFeatures(1<<18)
    val dfTf = tf.transform(df)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(2)
    val dfIdf = idf.fit(dfTf)

    val stringIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexed").setHandleInvalid("skip")
//    val dfIndx = stringIndexer.fit(dfIdf)

    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setInitMode("k-means||")
      .setInitSteps(5)
      .setK(5)
      .setMaxIter(20)
      .setPredictionCol("prediction")
      .setSeed(2019)


    val stages = Array(tf, idf)
    val pipeLine = new Pipeline()
      .setStages(stages)

    val tfIdf = pipeLine.fit(df).transform(df)
    tfIdf.show(10)

    val model = kmeans.fit(tfIdf)
    val wcss = model.computeCost(tfIdf)
    println(s"Set sum of squared errors= $wcss")

    // 打印中心点
    println("cluster centers:")
    val cent = model.clusterCenters
    print(cent.length)
    // 就算是toSparse了（转换为稠密向量），还是有相当多的值，为什么？
    /**
      *  如果有500篇文章，每篇文章有500个词，那么词的总数会相当多，比如3000个词
      *  那么对每篇文章都会有一个3000长度的向量，对单个向量转换为稠密向量会小很多，但是中心点是所有向量加到一起计算的，
      *  就算转换为稠密向量也不会小很多甚至和转换之前一样
      */
    // [0 1 1 1 0 0 0 0 0 0 0 ... ](3000)
    cent.map(x=>x.toSparse).foreach(println)
  }
}
