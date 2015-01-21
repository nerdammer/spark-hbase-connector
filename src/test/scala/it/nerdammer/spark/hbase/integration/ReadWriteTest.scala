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
class ReadWriteTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

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

  "reading" should "work after writing" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(1 to 100).map(i => (i.toString, i.toString))


    data.toHBaseTable(table).toColumns("column1").inColumnFamily(columnFamily).save()

    val count = sc.hbaseTable[(String, String)](table)
      .select("column1")
      .inColumnFamily(columnFamily)
      .count

    sc.stop

    count should be (100)

  }

  "reading" should "get the written values" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    try {
      sc.parallelize(1 to 1000)
        .map(i => ("STR-" + i.toString, i.toString, i))
        .toHBaseTable(table).toColumns("column2", "column3").inColumnFamily(columnFamily).save()

      val count = sc.hbaseTable[(String, String, Int)](table).inColumnFamily(columnFamily).select("column2", "column3")
        .filter(t => t._3 % 2 == 0)
        .filter(t => t._2.toInt % 4 == 0)
        .count

      count should be(250)

    } finally {
      sc.stop
    }

  }

  // TODO clear message for unknown columns
}
