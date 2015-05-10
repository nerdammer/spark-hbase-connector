package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf

case class HBaseSparkConf (
  hbaseHost: String = HBaseSparkConf.DefaultHBaseHost,
  hbaseRootDir: String = HBaseSparkConf.DefaultHBaseRootDir) extends Serializable {

  def createHadoopBaseConfig() = {
    val conf = HBaseConfiguration.create
    conf.setBoolean("hbase.cluster.distributed", true)
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf.set("hbase.rootdir", hbaseRootDir)
    conf.set("hbase.zookeeper.quorum", hbaseHost)

    conf
  }
}

object HBaseSparkConf extends Serializable {
  val DefaultHBaseHost = "hbase"
  val DefaultHBaseRootDir = "/hbase"

  def fromSparkConf(conf: SparkConf) = {
    HBaseSparkConf(
      hbaseHost = conf.get("spark.hbase.host", DefaultHBaseHost),
      hbaseRootDir = conf.get("spark.hbase.root.dir", DefaultHBaseRootDir)
    )
  }
}
