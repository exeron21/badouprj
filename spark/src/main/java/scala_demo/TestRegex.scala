package scala_demo

object TestRegex {

  def main(args: Array[String]): Unit = {
    val regex = """local\[([0-9]+|\*)\]""".r
    val results = regex.findAllMatchIn("[local[3]]")
    for (result <- results)
      println(result)
  }

}
