package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object ReceiverWC {
  def main(args: Array[String]): Unit = {

    // StreamingContext, zk, groupId, topic ,storageLevel
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("receiverWC")
    val ssc = new StreamingContext(conf, Seconds(10))

    val prdd = KafkaUtils.createStream(ssc,
    "master:2181,slave1:2181,slave2:2181",
    "g1",
    Map("badou"-> 3)).map(_._2)

    prdd.foreachRDD(rdd=>{
      rdd.map(x=>(x,1)).reduceByKey(_+_).foreach(println)
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
