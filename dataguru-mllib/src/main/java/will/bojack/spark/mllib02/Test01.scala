package will.bojack.spark.mllib02
import breeze.linalg._
import breeze.numerics._
import com.github.fommil.netlib.{BLAS=>NetlibBLAS, F2jBLAS}

object Test01 {

  def main(args: Array[String]): Unit = {
//    val m1 = DenseMatrix.zeros[Double](3,4)
//    DenseVector.tabulate(34)
//    val dd = NetlibBLAS.getInstance().dot(m1)
    println(NetlibBLAS.getInstance().getClass.getName)
  }


  def sigmoid(d:DenseVector[Double]) = {
    val len = d.toArray.size
    val dd = DenseVector.fill(len){-1}
  }
}
