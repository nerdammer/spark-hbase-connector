package it.nerdammer.spark.hbase

import org.scalatest.{FlatSpec, Matchers}

class HBaseReaderBuilderTest extends FlatSpec with Matchers {



  "ranges" should "be computed correctly" in {
    class TestConversions extends HBaseReaderBuilderConversions

    val bui = new HBaseReaderBuilder[String](null, table="table", salting = Seq("01", "02", "03"), startRow = Some("20150101"), stopRow = Some("20150201"))

    val conv = new TestConversions
    val builders = conv.getSaltedBuilders(bui)

    val rows = builders.map(b => (b.startRow, b.stopRow)).sortBy(_._1)
    rows(0) should be (Some("0120150101"), Some("0120150201"))
    rows(1) should be (Some("0220150101"), Some("0220150201"))
    rows(2) should be (Some("0320150101"), Some("0320150201"))
  }

  "ranges" should "be computed correctly with missing startRow" in {
    class TestConversions extends HBaseReaderBuilderConversions

    val bui = new HBaseReaderBuilder[String](null, table="table", salting = Seq("01", "02", "03"), stopRow = Some("20150201"))

    val conv = new TestConversions
    val builders = conv.getSaltedBuilders(bui)

    val rows = builders.map(b => (b.startRow, b.stopRow)).sortBy(_._1)
    rows(0) should be (Some("01"), Some("0120150201"))
    rows(1) should be (Some("02"), Some("0220150201"))
    rows(2) should be (Some("03"), Some("0320150201"))
  }

  "ranges" should "be computed correctly with missing stopRow" in {
    class TestConversions extends HBaseReaderBuilderConversions

    val bui = new HBaseReaderBuilder[String](null, table="table", salting = Seq("01", "02", "03"), startRow = Some("20150101"))

    val conv = new TestConversions
    val builders = conv.getSaltedBuilders(bui)

    val rows = builders.map(b => (b.startRow, b.stopRow)).sortBy(_._1)
    rows(0) should be (Some("0120150101"), Some("02"))
    rows(1) should be (Some("0220150101"), Some("03"))
    rows(2) should be (Some("0320150101"), None)
  }

  "ranges" should "not contain salting" in {
    class TestConversions extends HBaseReaderBuilderConversions

    val bui = new HBaseReaderBuilder[String](null, table="table", salting = Seq("01", "02", "03"), stopRow = Some("20150201"))

    val conv = new TestConversions
    val builders = conv.getSaltedBuilders(bui)

    val saltSize = builders.count(b => b.salting.nonEmpty)
    saltSize should be (0)
  }


}
