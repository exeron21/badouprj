package badou.c05

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Badou0402 {
  def main(args: Array[String]): Unit = {
    // 定义jieba分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxsize","800")

    // 建立一个sparkSession并传入定义好的conf
    val spark = SparkSession.builder()
      .appName("jiebaUDF")
      .master("local[4]")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    // 定义结巴分词的分词内容，写一个UDF，传入的是DATaFrame，输入的也是DataFrame
    //
    import org.apache.spark.sql.functions._
    def jiebaSeg(df:DataFrame,colname:String):DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jiebaUDF = udf{(sentence:String)=>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
          .filter(_.length>1).mkString("/")
      }
      df.withColumn("seg",jiebaUDF(col(colname)))
    }

    val df = spark.sql("select sentence,label from badou.news limit 100")
    val dfSeg = jiebaSeg(df,"sentence")
    dfSeg.show()
    dfSeg.write.mode("overwrite").saveAsTable("news_jieba")
  }
}
