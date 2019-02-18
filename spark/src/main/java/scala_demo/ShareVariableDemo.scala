package scala_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object ShareVariableDemo {
  var count = 0
  def main(args: Array[String]): Unit = {
    // 创建spark配置对象
    val conf = new SparkConf
    conf.setAppName("wordcountapp")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    sc.parallelize(1 to 20).map(e => {
      var str:String = java.net.InetAddress.getLocalHost.getHostAddress
      str += " : " + Thread.currentThread().getName + "\r\n"
      val sock = new java.net.Socket("master", 8888)
      val out = sock.getOutputStream
      out.write(str.getBytes())
      out.flush
      out.close
      sock.close
      e
    }).reduce(_+_)



  }
}
