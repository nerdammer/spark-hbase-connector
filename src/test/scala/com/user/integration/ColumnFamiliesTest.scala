package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ColumnFamiliesTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val tables: Seq[String] = Seq(UUID.randomUUID().toString)
  val columnFamilies: Seq[String] = Seq(UUID.randomUUID().toString, UUID.randomUUID().toString)

  override def beforeAll() = tables foreach {IntegrationUtils.createTable(_, columnFamilies)}

  override def afterAll() = tables foreach {IntegrationUtils.dropTable(_)}

  "reading and writing" should "work with different column families" in {

    val sc = IntegrationUtils.sparkContext

    val data = sc.parallelize(1 to 100).map(i => (i.toString, i.toString, i))


    data.toHBaseTable(tables(0))
      .toColumns("column1", columnFamilies(1) + ":column2")
      .inColumnFamily(columnFamilies(0))
      .save()

    val count1 = sc.hbaseTable[String](tables(0))
      .select(columnFamilies(0) + ":column1")
      .count

    val sum2 = sc.hbaseTable[Int](tables(0))
      .select("column2")
      .inColumnFamily(columnFamilies(1))
      .fold(0)(_ + _)

    count1 should be (100)
    sum2 should be (5050)
  }


}
