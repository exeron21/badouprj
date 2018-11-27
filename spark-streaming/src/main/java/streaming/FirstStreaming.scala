package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 第一个Spark Streaming例子
  */
object FirstStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("streaming-1")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val lines = ssc.socketTextStream("192.168.4.21", 9999)
    val words = lines.flatMap(_.split(" "))
//    val wordCount = words.map((_,1)).reduceByKey(_+_)
    val wordCount = words.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(30), Seconds(3))
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
