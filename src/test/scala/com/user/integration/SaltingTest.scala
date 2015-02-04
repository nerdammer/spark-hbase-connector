package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SaltingTest extends FlatSpec with Matchers with BeforeAndAfterAll with Serializable {

  val table: String = UUID.randomUUID().toString
  val columnFamily: String = "cf"

  override def beforeAll() = IntegrationUtils.createTable(table, columnFamily)

  override def afterAll() = IntegrationUtils.dropTable(table)

  "salting" should "be used during writes" in {

    val sc = IntegrationUtils.sparkContext

    sc.parallelize(1 to 1000)
      .map(i => (IntegrationUtils.pad(i.toString, 5), "Hi"))
      .toHBaseTable(table)
      .inColumnFamily(columnFamily)
      .toColumns("col")
      .withSalting((0 to 9).map(s => s.toString))
      .save()

    val rdd = sc.hbaseTable[(String, String)](table)
      .select("col")
      .inColumnFamily(columnFamily)
      .withStartRow("00501")
      .withSalting((0 to 9).map(s => s.toString))

    rdd.count should be (500)

    rdd.filter(t => t._1.length == 6).count should be (500)

  }



}
