package streaming.loganaylse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("logAnasylse-test")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("E:/checkpoint")
    val group = "g2"
    val topic = "streaming"
    val ipSeg = Array("master","slave1","slave2")
    val ipStr = ipSeg.map(_ + ":2181").mkString(",")
    val toMap = Array(topic).map((_, 3)).toMap

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val myfunc = (datehour:String, curr:Option[Long], state:State[Long])=> {
      val value:Long = curr.getOrElse(0L) + state.getOption().getOrElse(0L)
      state.update(value)
      (datehour, value)
    }
    val funcSpec = StateSpec.function(myfunc)
    val lines = KafkaUtils.createStream(ssc, ipStr, group, toMap)
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
