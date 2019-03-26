package streaming

import com.alibaba.fastjson.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.ConversionUtil

object AccessLogHandler {
  def main(args: Array[String]): Unit = {
    // sparkStreamingContext
    val conf = new SparkConf().setAppName("firstStreaming")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(6))
    ssc.checkpoint("e:/checkpoint")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    Logger.getLogger("org.apache.hive").setLevel(Level.ERROR)

    val zk = Array("master", "slave1", "slave2").map(_ + ":2181").mkString(",")
    val groupId = "g2"
    val topicSet = "streaming".split(",").toSet
    val topics = topicSet.map((_, 2)).toMap
    val lines = KafkaUtils.createStream(ssc, zk, groupId, topics).map(_._2)
//    val lines = ssc.socketTextStream("master", 9999)
    // streaming更新变量状态的方法一：
    val updateFunc = (curr:Seq[Long], pref:Option[Long])=>{
      val c = curr.sum
      val p = pref.getOrElse(0L)
      Some(c + p)
    }

    def rddToDataFrame(rdd:RDD[String]):DataFrame={
      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("accessLogHandler")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._
      rdd.map(x=>{
        ConversionUtil.convertAccessLog(x)
      }).toDF()
    }



   lines.foreachRDD(rdd=>{
      val df = rddToDataFrame(rdd)
      println("writing badou.accesslog")

      df.write.mode(SaveMode.Append).insertInto("badou.accesslog")
    })
    val haha = lines.map(x=>{
      val arr = x.toString().split(" ")
      (arr(0), 1L)
    }).updateStateByKey(updateFunc)

    val spark = SparkSession.builder()
        .master("local[*]")
        .appName("accessLog")
        .enableHiveSupport()
        .getOrCreate()
    haha.print()


//    haha.map(_._2).reduce(_+_).print()
    // streaming更新变量状态的方法二：
    /**
    val myfunc = (sth:String, curr:Option[Long], state:State[Long])=>{
      val sum = curr.getOrElse(0L) + state.getOption().getOrElse(0L)
      state.update(sum)
      (sth, sum)
    }
    val stateSpec = StateSpec.function(myfunc)
    words.mapWithState(stateSpec)
    */
    ssc.start()
    ssc.awaitTermination()
  }
}
