package it.nerdammer.spark.hbase

import com.user.integration.IntegrationUtils
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class HBaseSparkConfTest extends FlatSpec with Matchers {

  "A default HBaseSparkConf" should "have a empty host property" in {

    val conf = new HBaseSparkConf

    conf.hbaseHost should be (None)
  }

  "A default HBaseSparkConf object" should "have a host property set to localhost without other configurations" in {

    val hbaseConf = HBaseSparkConf.fromSparkConf(new SparkConf).createHadoopBaseConfig()
    hbaseConf.get(HConstants.ZOOKEEPER_QUORUM) should be (HBaseSparkConf.DefaultHBaseHost)
  }

  "the hbase-site.xml file" should "be loaded from the classpath if available" in {

    val hbaseConf = HBaseSparkConf.fromSparkConf(new SparkConf()).createHadoopBaseConfig()
    assert(hbaseConf.getInt("file.stream-buffer-size", -1) == 8000) // set in test hbase-site.xml file

  }

  "the host inside the hbase-site.xml file" should "not be overriden with localhost" in {

    val hbaseConf = new HBaseSparkConf(hbaseXmlConfigFile = "alternative-hbase-site.xml").createHadoopBaseConfig()
    hbaseConf.get(HConstants.ZOOKEEPER_QUORUM) should be ("anotherhost")

  }

  "the host passed as argument to spark" should "have always precedence" in {

    val hbaseConf = new HBaseSparkConf(hbaseXmlConfigFile = "alternative-hbase-site.xml", hbaseHost = Some("mysparkHost")).createHadoopBaseConfig()
    hbaseConf.get(HConstants.ZOOKEEPER_QUORUM) should be ("mysparkHost")

  }
}
