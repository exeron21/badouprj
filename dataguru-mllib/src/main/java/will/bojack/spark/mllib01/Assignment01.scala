package will.bojack.spark.mllib01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过Spark读取安装列表数据，并且统计数据总行数、用户数量、日期有哪几天
  */
object Assignment01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Assignment01")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:///home/hdp/data/dataguru-mllib/*.gz")

    val lineCount = rdd.count()  // 6503485
    val userCount = rdd.map(_.split("\t")).map(x=>x(1)).distinct().count()  // 108238
    val dates = rdd.map(_.split("\t")(0)).distinct()  // 26
  }

}
