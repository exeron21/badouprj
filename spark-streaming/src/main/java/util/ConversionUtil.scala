package util

import scala.beans.BeanProperty

/**
  *
  * log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
  *                   '$status $body_bytes_sent "$http_referer" '
  *                   '"$http_user_agent" "$http_x_forwarded_for"';
  *
  * log_format语法格式及参数语法说明如下:
  * log_format    <NAME>    <Strin­­­g>;
  * 关键字         格式标签   日志格式
  *
  * 关键字：其中关键字error_log不能改变
  * 格式标签：格式标签是给一套日志格式设置一个独特的名字
  * 日志格式：给日志设置格式
  *
  * log_format格式变量：
  * $remote_addr  #记录访问网站的客户端地址
  * $remote_user  #远程客户端用户名
  * $time_local  #记录访问时间与时区
  * $request  #用户的http请求起始行信息
  * $status  #http状态码，记录请求返回的状态码，例如：200、301、404等
  * $body_bytes_sent  #服务器发送给客户端的响应body字节数
  * $http_referer  #记录此次请求是从哪个连接访问过来的，可以根据该参数进行防盗链设置。
  * $http_user_agent  #记录客户端访问信息，例如：浏览器、手机客户端等
  * $http_x_forwarded_for  #当前端有代理服务器时，设置web节点记录客户端地址的配置，此参数生效的前提是代理服务器也要进行相关的x_forwarded_for设置
  *
  */
object ConversionUtil {
  case class AccessLog(@BeanProperty remoteAddr:String,
                      @BeanProperty remoteUser:String,
                      @BeanProperty timeLocal:String,
                      @BeanProperty request:String,
                      @BeanProperty status:Int,
                      @BeanProperty bodyBytesSent:Int,
                      @BeanProperty httpReferer:String,
                      @BeanProperty httpUserAgent:String,
                      @BeanProperty httpXForwardedFor:String)

  // 194.237.142.21 - - [19/Sep/2013:06:26:36 +0000] "GET /wp-content/uploads/2013/07/rstudio-login.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
  def convertAccessLog(line:String):AccessLog={
    val arr = line.split("\"")
    val feg1 = arr(0).split("\\[|\\]")
    val tmp = feg1(0)
    val remoteAddr = tmp.split(" ")(0)
    val timeLocal = feg1(1)
    return AccessLog(remoteAddr,null,timeLocal,null,0,0,null,null,null)
  }

  def main(args: Array[String]): Unit = {
    val arr = ConversionUtil.convertAccessLog("194.237.142.21 - - [19/Sep/2013:06:26:36 +0000] \"GET /wp-content/uploads/2013/07/rstudio-login.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/4.0 (compatible;)\"")
    println(arr)
  }

}
