package it.nerdammer.spark.hbase

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by Nicola Ferraro on 10/01/15.
 */
class HBaseRDDTest extends FlatSpec with Matchers {

  "a spark context" should "have a method named hbaseTable" in {

    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    sc.hbaseTable[String]("connector").withColumnFamily("test1").withColumns("c2")
      .foreach({
      case a => {
        println(a)
      }
    })


  }



}
