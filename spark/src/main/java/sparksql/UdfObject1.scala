package sparksql

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._

object UdfObject1 {
  def main(args: Array[String]): Unit = {
    val ds = new DenseVector(Array(0.11,0.82,0.43,0.24,0.65))

//    val xmz = pSort1(ds)
    val xmy = pSort2(ds)

    print(xmy)

    val pSort1 = udf{(x:DenseVector) =>
      val tmp = x.toArray
      tmp.zipWithIndex.sortWith(_._1 > _._1).array(0)
    }
  }

  def pSort2 (x:DenseVector):Int = {
    val tmp = x.toArray
    tmp.zipWithIndex.sortWith(_._1 > _._1).array(0)._2
  }

}
