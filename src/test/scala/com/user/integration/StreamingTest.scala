package com.user.integration

import java.util.UUID

import it.nerdammer.spark.hbase._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

class StreamingTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val tables: Seq[String] = Seq(UUID.randomUUID().toString)
  val columnFamilies: Seq[String] = Seq(UUID.randomUUID().toString)

  override def beforeAll() = tables foreach {IntegrationUtils.createTable(_, columnFamilies)}

  override def afterAll() = tables foreach {IntegrationUtils.dropTable(_)}

  "streaming" should "work with the library" in {

    val sc = IntegrationUtils.sparkContext

    val ssc = new StreamingContext(sc, Seconds(1))

    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    val table = tables(0)
    val cf = columnFamilies(0)

    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x, x))
    mappedStream
      .foreachRDD(rdd => rdd.toHBaseTable(table)
          .inColumnFamily(cf)
          .toColumns("col1")
          .save()
      )


    ssc.start()

    (0 to 5).map(i => {
      rddQueue += ssc.sparkContext.makeRDD(10*i until 10*i+10, 10)
      Thread.sleep(1000)
    })


    Thread.sleep(5000)

    ssc.stop(false)

    val sum = sc.hbaseTable[(Int)](table)
      .select("col1")
      .inColumnFamily(cf)
      .reduce(_ + _)


    sum should be {59*60/2}

  }


}
