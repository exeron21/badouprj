package streaming

import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object DirectWC {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setAppName("directWC")

    val Array(brokers,topics) = args
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map("bootstrap.servers"->brokers)
    val topicSet = topics.split(",").toSet
    // 不要直接用"xxx".toSet，会生成set[Char]类型的对象，把字符串中的每个字符拆出来放到set里了
    // ssc , kafkaParams, topicSet
//    val message = KafkaUtils.createDirectStream(ssc,kafkaParams,topic)
    val message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)

    val lines = message.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x=>(x,1L)).reduceByKey(_+_)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
