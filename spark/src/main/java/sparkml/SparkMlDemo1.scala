/*package sparkml

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._


object SparkMlDemo1 {
  def main(args: Array[String]): Unit = {
    // 数据目录
    val dir = "E:\\spark_ml\\wines\\"

    val ss = SparkSession.builder
      .appName("sparkmldemo")
      .master("local[*]")
      .getOrCreate

    val sc = ss.sparkContext
    // 定义样例类
    case class Wine(FixedAcidity: Double, VolatileAcidity: Double,
                    CitricAcid: Double, ResidualSugar: Double, Chlorides: Double,
                    FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH:
                    Double, Sulphates: Double, Alcohol: Double, Quality: Double)

    // 变换数据
    val wineDataRdd = sc.textFile(dir + "winequality-red.csv").map(_.split(";"))
    val firstRow = wineDataRdd.first

    val wineDataRdd1 = wineDataRdd.map(w =>
      Wine(w(0).toDouble,
        w(1).toDouble,
        w(2).toDouble,
        w(3).toDouble,
        w(4).toDouble,
        w(5).toDouble,
        w(6).toDouble,
        w(7).toDouble,
        w(8).toDouble,
        w(9).toDouble,
        w(10).toDouble,
        w(11).toDouble
      ))
    import ss.implicits._
    val trainingDF = wineDataRdd1.map(w => (w.Quality,
      Vectors.dense(w.FixedAcidity, w.VolatileAcidity, w.CitricAcid,
        w.ResidualSugar, w.Chlorides, w.FreeSulfurDioxide, w.TotalSulfurDioxide,
        w.Density, w.PH, w.Sulphates, w.Alcohol))).toDF("label", "features")


    val st = StructType(List(StructField("label",DoubleType), new StructField("feature",DoubleType)))
//    val trainingDF = ss.createDataFrame(trainingRDD)
//    val trainingDF = ss.createDataFrame(wineDataRdd2, st)
    trainingDF.show

    println("===========================")

    val lr = new LinearRegression
    lr.setMaxIter(10)
    // 通过线性回归拟合训练数据，生成模型
    val model = lr.fit(trainingDF)

    val testDF = ss.createDataFrame(Seq((5.0, Vectors.dense(7.4,
      0.7, 0.0, 1.9, 0.076, 25.0, 67.0, 0.9968, 3.2, 0.68, 9.8)), (5.0,
      Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0, 0.9978, 3.51, 0.56,
        9.4)), (7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0, 0.9968,
      3.36, 0.57, 9.5)))).toDF("label", "features")

    testDF.show
    testDF.createOrReplaceTempView("test")

    val tested = model.transform(testDF).select("features", "label", "prediction")
    tested.show
  }
}*/
