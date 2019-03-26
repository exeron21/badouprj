package badou.c05

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object JiebaKry2 {
  def main(args: Array[String]): Unit = {
    // 定义结巴分词的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize", "800")
    // 生成sparkSession
    val spark = SparkSession.builder()
      .config(conf)
      .appName("jiebaKry")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // create table news_noseg as select split(regexp_replace(sentence,' ',''),'##@@##')[0] as sentence,split(regexp_replace(sentence,' ',''),'##@@##')[1] as label from news_seg;
    import org.apache.spark.sql.functions._
    def jiebaSeg(df:DataFrame, colName:String):DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)

      val jiebaUDF = udf{(sentence:String) =>
        val segV = seg.value
        segV.process(sentence.toString(), SegMode.INDEX)
      }
      return null
    }
  }
}
