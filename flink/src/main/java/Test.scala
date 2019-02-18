import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Test {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("master", 9999)

    data.flatMap(_.split("\\s+")) // flatMap返回一个DataStream
      .map((_, 1L)) // 返回一个DataStream[Tuple2]，此时还不能使用countWindow和timeWindows，因为没有key.
      /**
        * keyBy返回keyedStream，后面才能使用timeWindow和countWindow
        * keyBy只是指定了key，数据结构没有变化 ，打印出来也和之前一样
        */
      .keyBy(0) // 设置DataStream的Key，返回一个keyedStream,参数就是key的位置，以0开始。可以指定多个key字段
      /** countWindow和timeWindow是类似的概念。
        * timeWindow意思就是多长时间翻滚一下，而countWindow是计数达到多少了翻滚一下
        * 与count相关的操作都是计数。 countWindow(n:Long)翻滚窗口
        * 注意keyedStream没有print方法，一般通过其他操作转换成DataStream再打印
        */
      /**
        * timeWindow(Time.seconds(n)) ： 设置n秒的翻滚窗口。每隔n秒处理一次在这n秒内接收到的所有数据
        * timeWindow(Time.seconds(n), Time.seconds(m)) : 设置n秒的翻滚窗口。但是每隔m秒处理一次
        *
        * countWindow(4) 意思是key达到4个了就处理一次，同时将计数清0
        *
        * 加上all的，就会忽略key，比如countWindowAll(5)，会从第一个接收的数据开始计数，计到5之后处理（也就是print）然后清0
        * 如输入：
        * a
        * b
        * c
        * d
        * e
        * 就会输出(a, 5)，哪怕后面几个key根本不是a
        *
        * timeWindowAll同理，从第1次接收的数据开始计数，计到5秒后处理（也就是print），不管key是什么，如：
        * a
        * b
        * c
        * d
        * ef
        * ...
        * 5秒后输出(a, n)  // n是这5秒内接收到的数据数量
        */
      .timeWindowAll(Time.of(5, TimeUnit.SECONDS))
      //      .countWindow(4L)
      //      .timeWindowAll(Time.seconds(5))
      //      .timeWindow(Time.seconds(10), Time.seconds(2))
      .sum(1)
      .print()
      .setParallelism(1)

    senv.execute("Flink Streaming")
  }
}