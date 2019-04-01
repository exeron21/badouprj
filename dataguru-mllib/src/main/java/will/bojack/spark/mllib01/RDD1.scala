package will.bojack.spark.mllib01

import org.apache.spark.{SparkConf, SparkContext}

object RDD1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("rdd1")
      .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)

    val data = Array(1,2,3,4,5,6,7,8)
    val rdd1 = sc.parallelize(data, 3)

    val rdd24 = sc.parallelize(Array((1, 1.0), (1, 2.0), (2, 4.0), (2, 5.0)))
    val combiner24 = rdd24.combineByKey(createCombiner = (v: Double) => (v: Double, 1),
      mergeValue = (c: (Double, Int), v: Double) => (c._1 + v, c._2 + 1),
      mergeCombiners = (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),2
    )
    combiner24.collect()
    val treeAggregate = rdd24.treeAggregate((0,0.0))(seqOp = (u, t) => (u._1 + t._1, u._2 + t._2),
      combOp = (u1, u2) => (u1._1 + u2._1, u1._2 + u2._2), 2)

    println(treeAggregate)

  }


  def makePair[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while(iter.hasNext) {
      val cur = iter.next
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }
}
