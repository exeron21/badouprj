package sparksql

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object JiebaKrp {
  def main(args: Array[String]): Unit = {
    // 定义jieba分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize", "800")
      .setJars(List("E:\\IdeaProjects\\hadoop-test\\spark-test\\target\\spark-test-1.0-SNAPSHOT.jar",
      "D:\\apache-maven-3.5.2\\repository\\com\\huaban\\jieba-analysis\\1.0.2\\jieba-analysis-1.0.2.jar",
      "D:\\apache-maven-3.5.2\\repository\\com\\alibaba\\fastjson\\1.2.17\\fastjson-1.2.17.jar"))
    // 建立SparkSession并传入定义好的conf
    val spark = SparkSession.builder()
      .master("spark://master:7077")
      .appName("jiebaUDF")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    // 定义jieba分词的方法，传入DataFrame，输出也是DATaFrame，多一列seg(分好词的一列)
    import org.apache.spark.sql.functions._

    // 参数是输入的dataframe，和列名
    def jieba_seg(df:DataFrame, colname:String):DataFrame={
      // 定义一个分词器
      val segmenter = new JiebaSegmenter
      // 广播这个分词器
      val seg = spark.sparkContext.broadcast(segmenter)
      // 定义jieba udf
      val jieba_udf = udf{(sentence:String) =>
        val segV = seg.value
        segV.process(sentence.toString,SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
          .filter(_.length>1).mkString("/")
      }
      df.withColumn("seg",jieba_udf(col(colname)))
    }

    val df = spark.sql("select sentence ,label from badou.news limit 300")
    val df_seg = jieba_seg(df,"sentence")
    df_seg.write.mode("overwrite").saveAsTable("badou.news_jieba")

  }
}
