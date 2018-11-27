package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length<2) {
      System.err.println("Usage: hdfswordcount <directory> <directory>")
    }
    val conf = new SparkConf().setAppName("hdfswordcount")

    val ssc = new StreamingContext(conf, Seconds(3))

//    val lines = ssc.textFileStream(args(0))
    val lines = ssc.socketTextStream("192.168.4.21", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).reduceByKey(_+_)
    wordCounts.print()
    wordCounts.saveAsTextFiles(args(1))
    ssc.start()
    ssc.awaitTermination()
  }
}
