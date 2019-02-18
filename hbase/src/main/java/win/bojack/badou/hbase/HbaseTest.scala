package win.bojack.badou.hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession


object HbaseTest {
  def main(args: Array[String]): Unit = {
    val zk_quorum = "master:2181,slave1:2181,slave2:2181"
    // 下面可以不用加端口，在jobconf中加端口也可以： jobconf.set("hbase.zookeeper.property.clientPort","2181")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("hbase+hive+spark")
      .enableHiveSupport()
      .getOrCreate()

    val rdd = spark.sql("select userid,orderid from badou.order_partition limit 200")
      .rdd

    rdd.map {row =>
      val userId = row(0).asInstanceOf[String]
      val orderId = row(1).asInstanceOf[String]

      var p = new Put(Bytes.toBytes(userId))
      p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(userId))
      p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(orderId))
      p
    }.foreachPartition{partition =>
      val jobconf = new JobConf(HBaseConfiguration.create())
      jobconf.set("hbase.zookeeper.quorum", zk_quorum)
      jobconf.set("zookeeper.znode.parent", "/hbase")

      jobconf.setOutputFormat(classOf[TableOutputFormat])

      val conn = ConnectionFactory.createConnection(jobconf)
      val t1 = conn.getTable(TableName.valueOf("ns1:t2"))
      import scala.collection.JavaConversions._
      t1.put(seqAsJavaList(partition.toSeq))
      conn.close()
    }
  }
}
