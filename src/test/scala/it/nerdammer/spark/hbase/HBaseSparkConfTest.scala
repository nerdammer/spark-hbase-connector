package it.nerdammer.spark.hbase

import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class HBaseSparkConfTest extends FlatSpec with Matchers {

  "A default HBaseSparkConf" should "have a host property set to localhost" in {

    val conf = new HBaseSparkConf

    conf.hbaseHost should be (HBaseSparkConf.DefaultHBaseHost)
  }

  "A default HBaseSparkConf object" should "have a host property set to localhost" in {

    val conf = HBaseSparkConf.fromSparkConf(new SparkConf)
    conf.hbaseHost should be (HBaseSparkConf.DefaultHBaseHost)
  }
}
