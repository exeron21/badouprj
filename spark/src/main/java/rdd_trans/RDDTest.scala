package rdd_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDDTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array((1,2,3),(2,4,9), (2,3,4), (3,4,5)))
    val rdd1 = rdd.groupBy(x=>x._1).mapValues(y=> {y.map(z=>{z._1+z._2+z._3}).sum})

    val rdd2 = rdd.map(x=>(x._1,Seq(x._1,x._2,x._3)))
    val rdd3 = rdd2.reduceByKey((x,y) => {Seq(x.toList.sum + y.toList.sum)})
    val rdd4 = rdd3.map(x=>{(x._1, x._2.sum)})

//    val rdd4 = sc.parallelize(Array((1,2),(2,3),(3,4),(4,5)))

//    val rdd4 = sc.parallelize(Seq((1,2),(2,9),(3,4),(3,5)))
//    rdd4.reduceByKey((x,y)=>x+y)
//    val rdd1 = rdd.map(x=>{
//      (x._1, x)
//    }).groupByKey().mapValues(x=>{
//      x.map(y=>{
//        y._1+y._2+y._3
//      })
//    })

    rdd1.foreach(println)
  }

}
