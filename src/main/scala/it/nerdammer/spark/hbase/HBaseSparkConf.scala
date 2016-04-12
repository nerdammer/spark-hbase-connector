package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.spark.SparkConf

case class HBaseSparkConf (
  hbaseHost: Option[String] = None,
  hbaseXmlConfigFile: String = "hbase-site.xml") extends Serializable {

  def createHadoopBaseConfig() = {
    val conf = HBaseConfiguration.create

    val xmlFile = Option(getClass.getClassLoader.getResource(hbaseXmlConfigFile))
    xmlFile.foreach(f => conf.addResource(f))

    hbaseHost.foreach(h => conf.set(HConstants.ZOOKEEPER_QUORUM, h))
    if(Option(conf.get(HConstants.ZOOKEEPER_QUORUM)).isEmpty)
      conf.set(HConstants.ZOOKEEPER_QUORUM, HBaseSparkConf.DefaultHBaseHost)

    conf
  }
}

object HBaseSparkConf extends Serializable {

  val DefaultHBaseHost = "localhost"

  def fromSparkConf(conf: SparkConf) = {
    HBaseSparkConf(
      hbaseHost = Option(conf.get("spark.hbase.host", null))
    )
  }
}
