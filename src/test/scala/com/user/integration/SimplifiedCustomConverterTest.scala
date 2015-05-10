package com.user.integration

import java.util.UUID

import com.user.integration.SimplifiedCustomConverterTest.MySimpleData
import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.{FieldReader, FieldReaderProxy, FieldWriter, FieldWriterProxy}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SimplifiedCustomConverterTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

   val tables: Seq[String] = Seq(UUID.randomUUID().toString)
   val columnFamilies: Seq[String] = Seq(UUID.randomUUID().toString)

   override def beforeAll() = tables foreach {IntegrationUtils.createTable(_, columnFamilies)}

   override def afterAll() = tables foreach {IntegrationUtils.dropTable(_)}



   "reading and writing" should "work with custom converters" in {

     val sc = IntegrationUtils.sparkContext

     val data = sc.parallelize(1 to 100).map(i => new MySimpleData(i, i, "Name" + i.toString))

     data.toHBaseTable(tables(0))
       .inColumnFamily(columnFamilies(0))
       .save()

     val read = sc.hbaseTable[MySimpleData](tables(0))
       .inColumnFamily(columnFamilies(0))


     read.filter(m => m.prg!=m.id).count() should be (0)
     read.filter(m => m.prg%2==0).count() should be (50)
     read.filter(m => m.name.startsWith("Name")).count() should be (100)
   }


 }

object SimplifiedCustomConverterTest extends Serializable {

  class MySimpleData(val id: Int, val prg: Int, val name: String) extends Serializable {

  }

  implicit def myDataWriter: FieldWriter[MySimpleData] = new FieldWriterProxy[MySimpleData, (Int, Int, String)] {

    override def convert(data: MySimpleData) = (data.id, data.prg, data.name)

    override def columns = Seq("prg", "name")
  }


  implicit def myDataReader: FieldReader[MySimpleData] = new FieldReaderProxy[(Int, Int, String), MySimpleData] {

    override def columns = Seq("prg", "name")

    override def convert(data: (Int, Int, String)) = new MySimpleData(data._1, data._2, data._3)
  }

}
