import org.apache.spark.{SparkConf, SparkContext}

object ScalaPI {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setMaster("spark://master:7077")
      .setJars(List("E:\\IdeaProjects\\hadoop-test\\spark-test\\target\\spark-test-1.0-SNAPSHOT.jar"))
      .setAppName("sparkPI")
    val sc = new SparkContext(conf)
    val n = 1000000
    val anafun = (x:Int) => {
      val x = math.random * 2 - 1
      val y = math.random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }
    sc.broadcast(anafun)
    val count = sc.parallelize(1 to n).map(anafun).reduce(_ + _)

    println(count * 4.0 / n)
  }
}
