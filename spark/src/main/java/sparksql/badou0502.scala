package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object badou0502 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
//      .master("spark://master:7077")
      .master("local")
      .appName("badou0502")
      .getOrCreate()
    val conf = new SparkConf()
      conf.setJars(List("E:\\IdeaProjects\\hadoop-test\\spark-test\\target\\spark-test-1.0-SNAPSHOT.jar"))
    def mulBy(x:(Double) => Double) = x(3)
    val bc = mulBy(_*3)
    import scala.math._
    println(bc)
  }
}
