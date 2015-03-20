package com.user.integration

import java.util.UUID


import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import it.nerdammer.spark.hbase._
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

    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x, x))
    mappedStream
      .toHBaseTable(tables(0))
      .inColumnFamily(columnFamilies(0))
      .toColumns("col1")
      .save()

    ssc.start()

    for (i <- 0 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(10*i until 10*i+10, 10)
      Thread.sleep(1000)
    }

    ssc.stop(false)

    val sum = sc.hbaseTable[(Int)](tables(0))
      .select("col1")
      .inColumnFamily(columnFamilies(0))
      .reduce(_ + _)


    sum should be {60*61/2}

  }


}
