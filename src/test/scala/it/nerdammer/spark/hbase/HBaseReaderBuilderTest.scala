package it.nerdammer.spark.hbase

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by Nicola Ferraro on 10/01/15.
 */
class HBaseReaderBuilderTest extends FlatSpec with Matchers {

  "a HBaseRDD" should "allow filtering and counting" in {

    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val ciccioCount = sc.hbaseTable[(String, String)]("connector")
      .select("c1", "c2")
      .inColumnFamily("test1")
      .filter(_._1=="ciccio")
      .count

    println("Cicci: " + ciccioCount)

  }



}
