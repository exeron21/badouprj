package will.bojack.spark.mllib01

import org.apache.spark.{SparkConf, SparkContext}

object Assignment02 {

  case class Package(date: String, user: String, packageName: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Assignment02")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filePath = "file:///home/hdp/data/dataguru-mllib/*.gz"
    val rdd = sc.textFile(filePath)

    val given = ("2016-04-01", "2016-04-02")

    val result1 = rdd.map(_.split("\t")).map(x => (x(1), Package(x(0), x(1), x(2))))
      .filter(x => filterDate(x._2.date, given))  // 744752
      .groupByKey().mapValues(x => {
      val rdd1 = x.filter(y => y.date.equals(given._1));
      val rdd2 = x.filter(y => y.date.equals(given._2));
      rdd2.toSet.diff(rdd1.toSet)
    }).filter(m=>m._2.nonEmpty)  // 9554

  }

  /**
    * 检查date是不是和given中的两个值中的一个相等
    *
    * @param date
    * @param given
    * @return
    */
  def filterDate(date: String, given: (String, String)): Boolean = {
    date.equals(given._1) || date.equals(given._2)
  }
}
