package test

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

object JsonAndCaseClass {
  case class Orders(@BeanProperty var user_id:String ,@BeanProperty var order_id:String) {
    def this() = this(" ", " ")
  }
  def main(args: Array[String]): Unit = {
    val msg = """{"order_id": "23", "user_id": "1902489"}"""
    val or = new Orders()
    val js = JSON.parseObject(msg, classOf[Orders])
    println(js)
  }
}
