package it.nerdammer.spark.hbase.integration

import java.util.UUID

import it.nerdammer.spark.hbase.{HBaseSparkConf, _}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Created by Nicola Ferraro on 20/01/15.
 */
class TypeConversionTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val table: String = UUID.randomUUID().toString
  val columnFamily: String = "cfconv"

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

  "type conversion" should "work" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    try {

      sc.parallelize(1 to 100)
        .map(i => (i.toString, i, i.toShort, i.toLong, i % 2 == 0, i.toDouble, i.toFloat, BigDecimal(i), i.toString))
        .toHBaseTable(table).toColumns("col-int", "col-sho", "col-lon", "col-boo", "col-dou", "col-flo", "col-big", "col-str")
        .inColumnFamily(columnFamily)
        .save()


      val retrieved = sc.hbaseTable[(String, Int, Short, Long, Boolean, Double, Float, BigDecimal, String)](table)
        .select("col-int", "col-sho", "col-lon", "col-boo", "col-dou", "col-flo", "col-big", "col-str")
        .inColumnFamily(columnFamily)
        .sortBy(_._1.toInt)
        .collect()

      val cmp = (1 to 100) zip retrieved

      cmp.foreach(p => {
        p._1 should be(p._2._2)
        p._1.toShort should be(p._2._3)
        p._1.toLong should be(p._2._4)
        (p._1 % 2 == 0) should be(p._2._5)
        p._1.toDouble should be(p._2._6)
        p._1.toFloat should be(p._2._7)
        BigDecimal(p._1) should be(p._2._8)
        p._1.toString should be(p._2._9)
      })

    } finally {
      sc.stop
    }

  }

  "type conversion" should "support nulls" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    try {

      sc.parallelize(1 to 100)


    } finally {
      sc.stop
    }

  }

  // TODO check nulls



}
