package it.nerdammer.spark.hbase.integration


import java.util.UUID

import it.nerdammer.spark.hbase.HBaseSparkConf
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import it.nerdammer.spark.hbase._

/**
 * Created by Nicola Ferraro on 20/01/15.
 */
class ReadTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val table: String = UUID.randomUUID().toString
  val columnFamily: String = "cf"

  override def beforeAll() = {
    val conf = HBaseSparkConf()
    val admin = new HBaseAdmin(conf.createHadoopBaseConfig())

    val tableDesc = new HTableDescriptor(TableName.valueOf(table))
    tableDesc.addFamily(new HColumnDescriptor(columnFamily))
    admin.createTable(tableDesc)
  }

  override def afterAll() = {
    val conf = HBaseSparkConf()
    val admin = new HBaseAdmin(conf.createHadoopBaseConfig())

    admin.disableTable(table)
    admin.deleteTable(table)
  }

  "reading" should "work" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    val ciccioCount = sc.hbaseTable[(String, String)](table)
      .select("c1", "c2")
      .inColumnFamily(columnFamily)
      .count

    sc.stop

    ciccioCount should be (0)


  }

  "reading" should "also work" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    val ciccioCount = sc.hbaseTable[(String, String)](table)
      .select("c1", "c2")
      .inColumnFamily(columnFamily)
      .count

    sc.stop

    ciccioCount should be (0)

  }


  /*"a HBaseRDD" should "allow filtering and counting" in {

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

  }*/

  /*"a HBaseRDD" should "allow selecting the row id" in {

    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val ciccioCount = sc.hbaseTable[(String, String)]("connector")
      .select("c1")
      .inColumnFamily("test1")
      .foreach(println)

  }*/

}
