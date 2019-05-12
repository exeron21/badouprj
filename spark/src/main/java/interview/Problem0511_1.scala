package interview

object Problem0511_1 {
  def main(args: Array[String]): Unit = {
    /**
      * 1
      * 2
      * 2.7
      * unexpected value
      * one
      * two
      * four
      */
    for (x <- Seq(1,2,2.7,3L,"one","two", "four")) {
      val str = x match {
        case 1 => x
        case _:Int =>x
        case _:Double =>x
        case "one" =>x
        case _:String =>x
        case _=>"unexpected value"
      }
      println(str)
    }
  }
}
