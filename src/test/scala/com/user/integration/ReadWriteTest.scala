package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ReadWriteTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val tables: Seq[String] = Seq(UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString)
  val columnFamily: String = "cf"

  override def beforeAll() = tables foreach {IntegrationUtils.createTable(_, columnFamily)}

  override def afterAll() = tables foreach {IntegrationUtils.dropTable(_)}

  "reading" should "work after writing" in {

    val sc = IntegrationUtils.sparkContext

    val data = sc.parallelize(1 to 100).map(i => (i.toString, i.toString))


    data.toHBaseTable(tables(0)).toColumns("column1").inColumnFamily(columnFamily).save()

    val count = sc.hbaseTable[(String, String)](tables(0))
      .select("column1")
      .inColumnFamily(columnFamily)
      .count

    count should be (100)

  }

  "reading" should "get the written values" in {

    val sc = IntegrationUtils.sparkContext


    sc.parallelize(1 to 1000)
      .map(i => (i.toString, i.toString, i))
      .toHBaseTable(tables(1)).toColumns("column2", "column3").inColumnFamily(columnFamily).save()

    val count = sc.hbaseTable[(String, String, Int)](tables(1)).inColumnFamily(columnFamily).select("column2", "column3")
      .filter(t => t._3 % 2 == 0)
      .filter(t => t._2.toInt % 4 == 0)
      .count

    count should be(250)

  }

  "reading" should "get the written values if I do not care about row id" in {

    val sc = IntegrationUtils.sparkContext


    sc.parallelize(1 to 1000)
      .map(i => ("STR-" + i.toString, i.toString, i))
      .toHBaseTable(tables(2)).toColumns("column2", "column3").inColumnFamily(columnFamily).save()

    val count = sc.hbaseTable[(String, Int)](tables(2)).inColumnFamily(columnFamily).select("column2", "column3")
      .filter(t => t._2 % 2 == 0)
      .filter(t => t._1.toInt % 4 == 0)
      .count

    count should be(250)
  }



  // TODO clear message for unknown columns
}
