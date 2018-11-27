package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ajsdlfasdf").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("D:\\data\\checkpoint")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val addFunc = (curValues:Seq[Long],preValueState:Option[Long])=>{
      val curCount = curValues.sum
      val preCount = preValueState.getOrElse(0L)
      Some(curCount+preCount)
    }

    val lines = ssc.socketTextStream("192.168.4.21",9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1L)).updateStateByKey[Long](addFunc)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
