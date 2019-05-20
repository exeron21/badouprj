import org.apache.flink.streaming.api.scala._

/**
  * 最简单的能跑的flink程序
  */
object Flink01 {
  def main(args: Array[String]): Unit = {
    val see = StreamExecutionEnvironment.getExecutionEnvironment

    // stream是一个DataStream，操作方式和Rdd差不多
    val stream = see.socketTextStream("master", 9999)
    stream.flatMap(_.split("\\s+"))
      .map((_, 1L))
      .keyBy(0)
      .sum(1)
      .print()

    see.execute("flink01")
  }

}
