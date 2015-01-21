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

  "reading" should "work" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(1 to 100).map(i => (i.toString, i.toString))


    data.toHBaseTable(table).toColumns("column").inColumnFamily(columnFamily).save()

    val count = sc.hbaseTable[(String, String)](table)
      .select("column")
      .inColumnFamily(columnFamily)
      .count

    sc.stop

    count should be (100)

  }

}
