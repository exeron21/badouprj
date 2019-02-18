package streaming

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

import scala.beans.BeanProperty

object ReceiverFromKafka {
  case class Orders(@BeanProperty var userId:String ,@BeanProperty var orderId:String) {
    def this() = this("", "")
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: hdfswordcount <directory> <directory>")
    }
    val Array(group_id, topic , exectime, dt, path) = args
    val zkHostIp = Array("21","22","23").map("192.168.4." + _ + ":2181").mkString(",")

    val numOfThreads = 1
    // 创建streamContext
    val conf = new SparkConf().setAppName("receiverFromKafka")
//      .setMaster("local[*]")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_,numOfThreads.toInt)).toMap
    // 通过receiver接收kafka数据
    // KafkaUtils.createStream 返回的值是：return DStream of (Kafka message key, Kafka message value)p
    val msgRdd = KafkaUtils.createStream(ssc, zkHostIp, group_id,topicMap).map(_._2)

    def rdd2DF(rdd: RDD[String]):DataFrame = {
      val spark = SparkSession.builder()
        .appName("Streaming from kafka")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode","nonstrict")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._
      rdd.map{x=>
        val msg = JSON.parseObject(x, classOf[Orders])
        Orders(msg.userId,msg.orderId)
      }.toDF()
    }
    msgRdd.foreachRDD(rdd=>{
      val df = rdd2DF(rdd)
      df.take(1).foreach(println _)
      df.withColumn("dt", lit(dt.toString))
        .write
        .mode(SaveMode.Append)
        .insertInto("badou.order_partition")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
