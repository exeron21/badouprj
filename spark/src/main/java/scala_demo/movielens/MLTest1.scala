package scala_demo.movielens

import org.apache.spark.{SparkConf, SparkContext}

object MLTest1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setMaster("local")
    conf.setAppName("MLTest1")
    val sc = new SparkContext(conf)

    val data = sc.textFile("e:\\ml-100k\\u.user")
    val user_fields = data.map(_.split("\\|"))
    // 统计用户数
    val user_count = user_fields.map(_(0)).count

    val sex_count = user_fields.map(_(2)).distinct().count()
    val occ_count = user_fields.map(_(3)).distinct().count()
    val zip_count = user_fields.map(_(4)).distinct().count()

    println("用户数量：" + user_count)
    println("性别数量：" + sex_count)
    println("职业数量：" + occ_count)
    println("邮编数量：" + zip_count)


  }
}
