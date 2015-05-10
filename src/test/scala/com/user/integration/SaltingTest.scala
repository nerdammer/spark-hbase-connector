package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.SparkContext._

class SaltingTest extends FlatSpec with Matchers with BeforeAndAfterAll with Serializable {

  val tables: Seq[String] = Seq(UUID.randomUUID().toString)
  val columnFamilies: Seq[String] = Seq(UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString)

  override def beforeAll() = tables foreach {IntegrationUtils.createTable(_, columnFamilies)}

  override def afterAll() = tables foreach {IntegrationUtils.dropTable(_)}

  "salting" should "be used during writes" in {

    val sc = IntegrationUtils.sparkContext

    sc.parallelize(1 to 1000)
      .map(i => (IntegrationUtils.pad(i.toString, 5), "Hi"))
      .toHBaseTable(tables(0))
      .inColumnFamily(columnFamilies(0))
      .toColumns("col")
      .withSalting((0 to 9).map(s => s.toString))
      .save()



    val rdd = sc.hbaseTable[(String, String)](tables(0))
      .select("col")
      .inColumnFamily(columnFamilies(0))
      .withStartRow("00501")
      .withSalting((0 to 9).map(s => s.toString))

    rdd.count should be (500)

    rdd.filter(t => t._1.length == 5).count should be (500)

  }

  "default salting" should "be deterministic (eg. hash salting)" in {

    val sc = IntegrationUtils.sparkContext

    sc.parallelize(1 to 1000)
      .map(i => (IntegrationUtils.pad(i.toString, 5), "Hi"))
      .toHBaseTable(tables(0))
      .inColumnFamily(columnFamilies(1))
      .toColumns("col")
      .withSalting((0 to 9).map(s => s.toString))
      .save()

    sc.parallelize(1 to 1000)
      .map(i => (IntegrationUtils.pad(i.toString, 5), "Replaced"))
      .toHBaseTable(tables(0))
      .inColumnFamily(columnFamilies(1))
      .toColumns("col")
      .withSalting((0 to 9).map(s => s.toString))
      .save()



    val rdd = sc.hbaseTable[String](tables(0))
      .select("col")
      .inColumnFamily(columnFamilies(1))
      .withSalting((0 to 9).map(s => s.toString))

    rdd.collect.forall(_ == "Replaced") should be (true)
  }

  "salting" should "not be returned during reads" in {

    val sc = IntegrationUtils.sparkContext

    sc.parallelize(1 to 1000)
      .map(i => (IntegrationUtils.pad(i.toString, 5), "Hi"))
      .toHBaseTable(tables(0))
      .inColumnFamily(columnFamilies(2))
      .toColumns("col")
      .withSalting((0 to 9).map(s => s.toString))
      .save()



    val keys = sc.hbaseTable[(String, String)](tables(0))
      .select("col")
      .inColumnFamily(columnFamilies(2))
      .withSalting((0 to 9).map(s => s.toString))
      .map(t => t._1)


    keys.map(v => v.length).collect.forall(_ == 5) should be (true)


    val keys2 = sc.hbaseTable[(String, String)](tables(0))
      .select("col")
      .inColumnFamily(columnFamilies(2))
      .map(t => t._1)

    keys2.map(v => v.length).collect.forall(_ == 6) should be (true)

  }



}
