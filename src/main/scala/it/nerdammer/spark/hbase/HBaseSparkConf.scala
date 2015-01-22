package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf

case class HBaseSparkConf private[hbase] (
  hbaseHost: String = HBaseSparkConf.DefaultHBaseHost,
  hbaseRootDir: String = HBaseSparkConf.DefaultHBaseRootDir) {

  def createHadoopBaseConfig() = {
    val conf = HBaseConfiguration.create
    conf.setBoolean("hbase.cluster.distributed", true)
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf.set("hbase.rootdir", hbaseRootDir)
    conf.set("hbase.zookeeper.quorum", hbaseHost)

    conf
  }
}

object HBaseSparkConf {
  val DefaultHBaseHost = "localhost"
  val DefaultHBaseRootDir = "/tmp"

  def fromSparkConf(conf: SparkConf) = {
    HBaseSparkConf(
      hbaseHost = conf.get("spark.hbase.host", DefaultHBaseHost),
      hbaseRootDir = conf.get("spark.hbase.root.dir", DefaultHBaseRootDir)
    )
  }
}
