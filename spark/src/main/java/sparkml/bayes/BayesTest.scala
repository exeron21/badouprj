package sparkml.bayes

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

    val df = ss.sql("select * from badou.news_noseg")
    df.show()
    //    调用结巴对新闻进行切词
    val df_seg = jiebaSeg(df,"sentence").select("seg","label")
    df_seg.show()

//    val df = ss.sql("select * from badou.news_seg1")
//    df.show()
    import ss.implicits._
    val df1 = df.rdd.map(x=>{val sen = x(0).toString.split(" ");(sen, x(1).toString)}).toDF("sentence", "label")
    val dfSeg = jiebaSeg(df, "sentence").select("seg", "label")

    // HashingTF是SparkML的一个Transformer，用途是对输入的词进行hash编码和TF统计
    // 用binary方式统计词频，只要出现过就是1，没出现过为0. 与之对应的是多项式统计（出现多少次就是多少）
    val tf = new HashingTF().setBinary(false).setInputCol("sentence").setOutputCol("rawFeatures")

    val dfTf = tf.transform(dfSeg).select("rawFeatures", "label")
    dfTf.show()
    // IDF是SparkML的一个Estimator，用途是统计IDF反文档频率
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(1)

    val idfModel = idf.fit(dfTf) // estimator必须要fit

    val dfTfidf = idfModel.transform(dfTf).select("features", "label")
    dfTfidf.show()

    // StringIndexer也是Estimator
    val stringIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexed").setHandleInvalid("error")

    val dfTfidfLab = stringIndexer.fit(dfTfidf).transform(dfTfidf)
    dfTfidfLab.show()

    val Array(train, test) = df1.randomSplit(Array(0.8, 0.2))

    // multinomial 多项式
    val nb = new NaiveBayes().setModelType("multinomial").setSmoothing(1.0).setFeaturesCol("features").setLabelCol("indexed").setPredictionCol("pred_label").setProbabilityCol("prob").setRawPredictionCol("rawPred")

    val nbModel = nb.fit(train)
    val pipeLine = new Pipeline()
    pipeLine.setStages(Array(tf, idf, stringIndexer,nb))
    val model = pipeLine.fit(train).transform(train)

    // 评估方式, 模型评估f1:
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("indexed")
      .setPredictionCol("pred_label")
      .setMetricName("f1")

    val f1Score = eval.evaluate(model)
    println("Test f1 score = " + f1Score)

    nbModel.save(modelPath)
    println(s"bayes model saved: $modelPath")
  }
}
