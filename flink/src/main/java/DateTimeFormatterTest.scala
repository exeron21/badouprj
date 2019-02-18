import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import scala.collection.mutable.ArrayBuffer

object DateTimeFormatterTest {
  def main(args: Array[String]): Unit = {
    val dtf:DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val haha:LocalDate= LocalDate.parse("20181212", dtf)
    val lst = getBetweenTwoDates("20181212", "20181223")
    println(lst.size)
    for (i<-lst) println(i)
  }

  def getBetweenTwoDates(d1:String, d2:String) : ArrayBuffer[String] = {
    val dtf:DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    var date1:LocalDate = LocalDate.parse(d1, dtf)
    val date2:LocalDate = LocalDate.parse(d2, dtf)
    var lst:ArrayBuffer[String] = new ArrayBuffer[String]
    // plus
    date1 = date1.minus(11, ChronoUnit.DAYS)
    while (date1.isBefore(date2)) {
      lst += date1.format(dtf)
      date1 = date1.plusDays(1)
    }
    lst
  }
}
