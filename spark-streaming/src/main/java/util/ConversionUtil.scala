package util

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

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
  case class AccessLog(remoteAddr:String)
  /**
  case class AccessLog(@BeanProperty var remoteAddr:String,
                      @BeanProperty var remoteUser:String,
                      @BeanProperty var timeLocal:String,
                      @BeanProperty var method:String,
                      @BeanProperty var request:String,
                      @BeanProperty var httpVersion:String,
                      @BeanProperty var status:Int,
                      @BeanProperty var bodyBytesSent:Int,
                      @BeanProperty var httpReferer:String,
                      @BeanProperty var httpUserAgent:String,
                      @BeanProperty var httpXForwardedFor:String)
               */

  // 194.237.142.21 - - [19/Sep/2013:06:26:36 +0000] "GET /wp-content/uploads/2013/07/rstudio-login.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
  /**
    * 处理逻辑：
    * 用分隔符" - "(两边有空格)分出remoteAddr
    * remoteUser忽略
    * 余下的部分用分隔符"\\[|\\]"分出timeLocal
    * 余下的部分用分隔符"\""分出
    * @param line
    * @return
    */
  def convertAccessLog(line:String):AccessLog={
    var arr = line.split(" - ")
    val remoteAddr = arr(0)
    /**
    var remain = arr(1)
    arr = remain.split("\\[|\\]", 3)
    val timeLocalTmp = arr(1)
    val date = convertStrToDate(timeLocalTmp, "dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
    val timeLocal = convertDateToStr(date, "yyyy-MM-dd:hh:mm:ss", Locale.CHINESE)
    arr = remain.split("\"")
    remain = arr(1)
    val request_ = remain.split(" ")
    val method = request_(0)
    val request = request_(1)
    val httpVersion = request_(2)
    remain = arr(2)
    val statusAndLength = arr(2)
    var status:Int = 200
    var bodyBytesSent:Int = 0

    try {
      status = statusAndLength.trim().split(" ")(0).toInt
    } catch {
      case e:Exception=>println(e)
    }
    try {
      bodyBytesSent = statusAndLength.trim().split(" ")(1).toInt
    } catch {
      case e:Exception=>println(e)
    }
    val httpReferer = arr(3)
    val userAgent = arr(5)
    return AccessLog(remoteAddr,null,timeLocal,method,request,httpVersion,status,bodyBytesSent,httpReferer,userAgent,null)
      */
    return AccessLog(remoteAddr)
  }

  def convertStrToDate(str:String, pattern:String, locale:Locale=Locale.CHINESE):Date={
    val sdf:SimpleDateFormat = new SimpleDateFormat(pattern, locale)
    return sdf.parse(str)
  }
  def convertDateToStr(date:Date, pattern:String, locale:Locale):String={
    val sdf:SimpleDateFormat = new SimpleDateFormat(pattern, locale)
    return sdf.format(date)
  }

  /**
    * 如果日期字符串中有英文的Sep（月）Tue（星期），转换的时候一定要加上Locale.ENGLISH
    * @param args
    */
  def main(args: Array[String]): Unit = {
//    val arr = ConversionUtil.convertAccessLog("194.237.142.21 - - [19/Sep/2013:06:26:36 +0000] \"GET /wp-content/uploads/2013/07/rstudio-login.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/4.0 (compatible;)\"")
//    println(arr)
//    val str = "12/Sep/2100"
//    val date = convertStrToDate(str, "dd/MMM/yyyy", Locale.ENGLISH)
//    println(date)
  }
}
