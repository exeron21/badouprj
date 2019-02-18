package streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectOffsetDem {
  def main(args: Array[String]): Unit = {
    // 将org.apache.spark下的日志级别设置成ERROR
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // 接收brokers,topics,consumer
    val Array(brokers, topics,consumer) = Array("192.168.174.134:9092","badou","group_bd")

    // 创建sparkConf对象
    val sparkConf = new SparkConf().setAppName("DirectOffsetDem")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "group.id"-> consumer)


    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc,kafkaParams,topicsSet)
    var offsetRanges = Array[OffsetRange]()

    messages.foreachRDD{rdd=>
      offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for(offset <- offsetRanges){
        km.commitOffsetsToZK(offsetRanges)
        print(s"${offset.topic} ${offset.partition} ${offset.fromOffset}  ${offset.untilOffset}")
        //        badou 0 2798598  2798627
      }
    }
    messages.map(_._2).map((_,1L)).reduceByKey(_+_).print

    ssc.start()
    ssc.awaitTermination()
  }
}