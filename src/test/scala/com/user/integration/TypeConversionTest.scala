package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TypeConversionTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val tables = Seq(UUID.randomUUID().toString, UUID.randomUUID().toString)
  val columnFamilies = Seq("cfconv", "cfconv2")

  override def beforeAll() = {
    (tables zip columnFamilies) foreach (t => {
      IntegrationUtils.createTable(t._1, t._2)
    })
  }

  override def afterAll() = {
    tables.foreach(table => IntegrationUtils.dropTable(table))
  }

  "type conversion" should "work" in {

    val sc = IntegrationUtils.sparkContext

    sc.parallelize(1 to 100)
      .map(i => (i.toString, i, i.toShort, i.toLong, i % 2 == 0, i.toDouble, i.toFloat, BigDecimal(i), i.toString))
      .toHBaseTable(tables(0)).toColumns("col-int", "col-sho", "col-lon", "col-boo", "col-dou", "col-flo", "col-big", "col-str")
      .inColumnFamily(columnFamilies(0))
      .save()


    val retrieved = sc.hbaseTable[(String, Int, Short, Long, Boolean, Double, Float, BigDecimal, String)](tables(0))
      .select("col-int", "col-sho", "col-lon", "col-boo", "col-dou", "col-flo", "col-big", "col-str")
      .inColumnFamily(columnFamilies(0))
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

  }

  "type conversion" should "support empty values" in {

    val sc = IntegrationUtils.sparkContext

    sc.parallelize(1 to 100)
      .map(i => (i.toString, i, None.asInstanceOf[Option[Short]]))
      .toHBaseTable(tables(1))
      .inColumnFamily(columnFamilies(1))
      .toColumns("myint", "myshort")
      .save()


    val chk = sc.hbaseTable[(String, Option[Int], Option[Short], Option[Long], Option[Boolean], Option[Double], Option[Float], Option[BigDecimal], Option[String])](tables(1))
      .inColumnFamily(columnFamilies(1))
      .select("myint", "myshort", "mynonexistentlong", "mynonexistentbool", "mynonexistentdouble", "mynonexistentfloat", "mynonexistentbigd", "mynonexistentstr")
      .filter(r => r match {
      case (s, Some(i), None, None, None, None, None, None, None) => true
      case _ => false
      })
      .count

    chk should be (100)

  }

}
