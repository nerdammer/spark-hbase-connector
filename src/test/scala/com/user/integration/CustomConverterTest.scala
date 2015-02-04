package com.user.integration

import java.util.UUID

import com.user.integration.CustomConverterTest.MyData
import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.{FieldReader, FieldWriter}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CustomConverterTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val tables: Seq[String] = Seq(UUID.randomUUID().toString)
  val columnFamilies: Seq[String] = Seq(UUID.randomUUID().toString)

  override def beforeAll() = tables foreach {IntegrationUtils.createTable(_, columnFamilies)}

  override def afterAll() = tables foreach {IntegrationUtils.dropTable(_)}



  "reading and writing" should "work with custom converters" in {

    val sc = IntegrationUtils.sparkContext

    val data = sc.parallelize(1 to 100).map(i => new MyData(i, i, "Name" + i.toString))

    data.toHBaseTable(tables(0))
      .inColumnFamily(columnFamilies(0))
      .save()

    val read = sc.hbaseTable[MyData](tables(0))
      .inColumnFamily(columnFamilies(0))


    read.filter(m => m.prg!=m.id).count() should be (0)
    read.filter(m => m.prg%2==0).count() should be (50)
    read.filter(m => m.name.startsWith("Name")).count() should be (100)
  }


}

object CustomConverterTest extends Serializable {

  class MyData(val id: Int, val prg: Int, val name: String) extends Serializable {

  }

  implicit def myDataWriter: FieldWriter[MyData] = new FieldWriter[MyData] {
    override def map(data: MyData): HBaseData =
      Seq(
        Some(Bytes.toBytes(data.id)),
        Some(Bytes.toBytes(data.prg)),
        Some(Bytes.toBytes(data.name))
      )

    override def columns = Seq("prg", "name")
  }


  implicit def myDataReader: FieldReader[MyData] = new FieldReader[MyData] {
    override def map(data: HBaseData): MyData = new MyData(
      Bytes.toInt(data.head.get),
      Bytes.toInt(data.drop(1).head.get),
      Bytes.toString(data.drop(2).head.get)
    )

    override def columns = Seq("prg", "name")
  }

}
