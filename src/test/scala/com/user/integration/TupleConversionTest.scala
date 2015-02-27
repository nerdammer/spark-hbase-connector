package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TupleConversionTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val tables = Seq(UUID.randomUUID().toString)
  val columnFamilies = Seq("tupleconv")

  override def beforeAll() = {
    (tables zip columnFamilies) foreach (t => {
      IntegrationUtils.createTable(t._1, t._2)
    })
  }

  override def afterAll() = {
    tables.foreach(table => IntegrationUtils.dropTable(table))
  }

  "tuple conversion" should "work" in {

    val sc = IntegrationUtils.sparkContext


    val t1 = sc.parallelize(1 to 100).map(r => (r, 1))

    t1.toHBaseTable(tables(0)).toColumns("c1")
        .inColumnFamily(columnFamilies(0))
        .save()

    val r1 = sc.hbaseTable[(Int, Int)](tables(0))
      .select("c1")
      .inColumnFamily(columnFamilies(0))

    r1.map(t => (t._1, t._2+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r2 = sc.hbaseTable[(Int, Int, Int)](tables(0))
      .select("c1", "c2")
      .inColumnFamily(columnFamilies(0))

    r2.map(t => (t._1, t._2+1, t._3+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r3 = sc.hbaseTable[(Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3")
      .inColumnFamily(columnFamilies(0))

    r3.map(t => (t._1, t._2+1, t._3+1, t._4+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r4 = sc.hbaseTable[(Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4")
      .inColumnFamily(columnFamilies(0))

    r4.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r5 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5")
      .inColumnFamily(columnFamilies(0))

    r5.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r6 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6")
      .inColumnFamily(columnFamilies(0))

    r6.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r7 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7")
      .inColumnFamily(columnFamilies(0))

    r7.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r8 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")
      .inColumnFamily(columnFamilies(0))

    r8.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, t._9+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r9 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9")
      .inColumnFamily(columnFamilies(0))

    r9.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, t._9+1, t._10+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r10 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10")
      .inColumnFamily(columnFamilies(0))

    r10.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, t._9+1, t._10+1, t._11+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r11 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11")
      .inColumnFamily(columnFamilies(0))

    r11.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, t._9+1, t._10+1, t._11+1, t._12+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r12 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12")
      .inColumnFamily(columnFamilies(0))

    r12.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, t._9+1, t._10+1, t._11+1, t._12+1, t._13+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r13 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13")
      .inColumnFamily(columnFamilies(0))

    r13.map(t => (t._1, t._2+1, t._3+1, t._4+1, t._5+1, t._6+1, t._7+1, t._8+1, t._9+1, t._10+1, t._11+1, t._12+1, t._13+1, t._14+1, 1))
      .toHBaseTable(tables(0)).toColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14")
      .inColumnFamily(columnFamilies(0))
      .save()

    val r14 = sc.hbaseTable[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tables(0))
      .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14")
      .inColumnFamily(columnFamilies(0))
      .take(1).head


    assert(r14._2==14)
    assert(r14._3==13)
    assert(r14._4==12)
    assert(r14._5==11)
    assert(r14._6==10)
    assert(r14._7==9)
    assert(r14._8==8)
    assert(r14._9==7)
    assert(r14._10==6)
    assert(r14._11==5)
    assert(r14._12==4)
    assert(r14._13==3)
    assert(r14._14==2)
    assert(r14._15==1)

  }


}
