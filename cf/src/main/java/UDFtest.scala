import org.apache.spark.sql.functions._

object UDFtest {
  def main(args: Array[String]): Unit = {
    val udf_add = udf((a:Int, b:Int) => a + b)

  }
}
