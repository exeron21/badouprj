object GetSparkParameter {
  def main(args: Array[String]): Unit = {
    val cores = Runtime.getRuntime().availableProcessors()
    println(cores)
  }

}
