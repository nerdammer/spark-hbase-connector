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

  "type conversion" should "work" in {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(1 to 100)


    data
      .map(i => (i.toString, i))
      .toHBaseTable(table).toColumns("col-int").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, i.toShort))
      .toHBaseTable(table).toColumns("col-sho").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, i.toLong))
      .toHBaseTable(table).toColumns("col-lon").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, i%2==0))
      .toHBaseTable(table).toColumns("col-boo").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, i.toDouble))
      .toHBaseTable(table).toColumns("col-dou").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, i.toFloat))
      .toHBaseTable(table).toColumns("col-flo").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, BigDecimal.apply(i)))
      .toHBaseTable(table).toColumns("col-big").inColumnFamily(columnFamily).save()

    data
      .map(i => (i.toString, i.toString))
      .toHBaseTable(table).toColumns("col-str").inColumnFamily(columnFamily).save()


    val retrieved = sc.hbaseTable[(String, Int, Short, Long, Boolean, Double, Float, BigDecimal, String)](table)
      .select("col-int", "col-sho", "col-lon", "col-boo", "col-dou", "col-flo", "col-big", "col-str")
      .inColumnFamily(columnFamily)
      .sortBy(_._1.toInt)
      .collect()

    val cmp = (1 to 100) zip retrieved

    val ok = cmp.count (p => {
      p._1.toInt == p._2._2 &&
        p._1.toShort == p._2._3
    })

    sc.stop

    ok should be (100)

  }



}
