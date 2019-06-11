package bayes

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BayesTest {
  def main(args: Array[String]): Unit = {

    val modelPath = "hdfs://model_path/bayes"
    val conf = new SparkConf().registerKryoClasses(Array(classOf[JiebaSegmenter])).set("spark.rpc.message.maxSize", "800")

    val ss = SparkSession.builder().config(conf).appName("BayesTest").enableHiveSupport().getOrCreate()

    def jiebaSeg(df: DataFrame, column: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = ss.sparkContext.broadcast(segmenter)
      val jiebaUdf = udf { sentence: String => {
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
      }
      }
      df.withColumn("seg", jiebaUdf(col(column)))
    }

    val df = ss.sql("select * from badou.news_seg")
    df.show()
    import ss.implicits._
    val df1 = df.rdd.map(x=>{val sen = x(2).toString.split(" ");(x(0).toString+x(1).toString, x(1).toString, sen)}).toDF("idx", "label", "sentence")
    // HashingTF:transformer
    val tf = new HashingTF().setBinary(false).setInputCol("sentence").setOutputCol("rawFeatures")
    // IDF:Estimator
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(1)
    // StringIndexer:Estimator
    val stringIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexed").setHandleInvalid("error")
    // NaiveBayes: Estimator
    val nb = new NaiveBayes().setModelType("multinomial").setSmoothing(1.0).setFeaturesCol("features").setLabelCol("indexed").setPredictionCol("pred_label").setProbabilityCol("prob").setRawPredictionCol("rawPred")
    val Array(train, test) = df1.randomSplit(Array(0.8, 0.2))

    val dfTf = tf.transform(df1)
    val dfIdf = idf.fit(dfTf).transform(dfTf)
    val dfSi = stringIndexer.fit(dfIdf).transform(dfIdf)
    val Array(train2, test2) = dfSi.randomSplit(Array(0.8, 0.2))
    val dfNb = nb.fit(train2).transform(test2)
    val pipeLine = new Pipeline()
    pipeLine.setStages(Array(tf, idf, stringIndexer,nb))
    val model = pipeLine.fit(train2.select("idx", "sentence","label")).transform(test2.select("idx","sentence","label"))

    // 评估方式, 模型评估f1:
    val eval = new MulticlassClassificationEvaluator().setLabelCol("indexed").setPredictionCol("pred_label").setMetricName("f1")

    val f1Score = eval.evaluate(model)
    println("Test f1 score = " + f1Score)
  }
}
