package com.user.integration

import it.nerdammer.spark.hbase._
import org.scalatest.{FlatSpec, Matchers}

class ConfigurationReadTest extends FlatSpec with Matchers {

  "the hbase-site.xml file" should "be loaded from the classpath if available" in {

    val sc = IntegrationUtils.sparkContext

    val hbaseConf = HBaseSparkConf.fromSparkConf(sc.getConf).createHadoopBaseConfig()
    assert(hbaseConf.getInt("file.stream-buffer-size", -1) == 8000) // set in test hbase-site.xml file

  }

}
