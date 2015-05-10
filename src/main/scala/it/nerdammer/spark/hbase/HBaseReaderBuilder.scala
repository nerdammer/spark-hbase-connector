package it.nerdammer.spark.hbase

import it.nerdammer.spark.hbase.conversion.FieldReader
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{NewHadoopRDD, RDD}

import scala.reflect.ClassTag

case class HBaseReaderBuilder [R: ClassTag] private[hbase] (
      @transient sc: SparkContext,
      private[hbase] val table: String,
      private[hbase] val columnFamily: Option[String] = None,
      private[hbase] val columns: Iterable[String] = Seq.empty,
      private[hbase] val startRow: Option[String] = None,
      private[hbase] val stopRow: Option[String] = None,
      private[hbase] val salting: Iterable[String] = Seq.empty
      )
      (implicit mapper: FieldReader[R], saltingProvider: SaltingProviderFactory[String]) extends Serializable {

    private[hbase] def withRanges(startRow: Option[String], stopRow: Option[String], salting: Iterable[String]) = copy(startRow = startRow, stopRow = stopRow, salting = salting)

    def select(columns: String*): HBaseReaderBuilder[R] = {
      require(this.columns.isEmpty, "Columns have already been set")
      require(columns.nonEmpty, "You should provide at least one column")

      this.copy(columns = columns)
    }

    def inColumnFamily(columnFamily: String) = {
      require(this.columnFamily.isEmpty, "Default column family has already been set")
      require(columnFamily.nonEmpty, "Invalid column family provided")

      this.copy(columnFamily = Some(columnFamily))
    }

    def withStartRow(startRow: String) = {
      require(startRow.nonEmpty, s"Invalid start row '$startRow'")
      require(this.startRow.isEmpty, "Start row has already been set")

      this.copy(startRow = Some(startRow))
    }

    def withStopRow(stopRow: String) = {
      require(stopRow.nonEmpty, s"Invalid stop row '$stopRow'")
      require(this.stopRow.isEmpty, "Stop row has already been set")

      this.copy(stopRow = Some(stopRow))
    }

    def withSalting(salting: Iterable[String]) = {
      require(salting.size > 1, "Invalid salting. Two or more elements are required")
      require(this.salting.isEmpty, "Salting has already been set")

      this.copy(salting = salting)
    }

}


trait HBaseReaderBuilderConversions extends Serializable {

  implicit def toHBaseRDD[R: ClassTag](builder: HBaseReaderBuilder[R])(implicit mapper: FieldReader[R], saltingProvider: SaltingProviderFactory[String]): RDD[R] = {
    if(builder.salting.isEmpty) {
      toSimpleHBaseRDD(builder)
    } else {
      //require(builder.startRow.nonEmpty || builder.stopRow.nonEmpty, "Salting can be used only together with startRow and/or stopRow")
      // Removed as salting can also be used to remove leading bytes from row keys

      val saltingLength = saltingProvider.getSaltingProvider(builder.salting).length

      val builders = getSaltedBuilders(builder)

      val rddSeq = builders.map(bui => toSimpleHBaseRDD(bui, saltingLength).asInstanceOf[RDD[R]]).toSeq
      val sc = rddSeq.head.sparkContext
      new HBaseSaltedRDD[R](sc, rddSeq)
    }
  }

  def toSimpleHBaseRDD[R: ClassTag](builder: HBaseReaderBuilder[R], saltingLength: Int = 0)(implicit mapper: FieldReader[R], saltingProvider: SaltingProviderFactory[String]): HBaseSimpleRDD[R] = {
    val hbaseConfig = HBaseSparkConf.fromSparkConf(builder.sc.getConf).createHadoopBaseConfig()

    hbaseConfig.set(TableInputFormat.INPUT_TABLE, builder.table)

    val columnNames = HBaseUtils.chosenColumns(builder.columns, mapper.columns)

    val columns = HBaseUtils.fullyQualifiedColumns(builder.columnFamily, columnNames)

    if(columns.nonEmpty) {
      hbaseConfig.set(TableInputFormat.SCAN_COLUMNS, columns.mkString(" "))
    }

    if(builder.startRow.nonEmpty) {
      hbaseConfig.set(TableInputFormat.SCAN_ROW_START, builder.startRow.get)
    }

    if(builder.stopRow.nonEmpty) {
      hbaseConfig.set(TableInputFormat.SCAN_ROW_STOP, builder.stopRow.get)
    }

    val rdd = builder.sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
      .asInstanceOf[NewHadoopRDD[ImmutableBytesWritable, Result]]

    new HBaseSimpleRDD[R](rdd, builder, saltingLength)
  }

  def getSaltedBuilders[R](builder: HBaseReaderBuilder[R]) = {
    val sortedSalting = builder.salting.toList.sorted map (str => Some(str))

    val ranges = sortedSalting zip (sortedSalting.drop(1) :+ None)

    ranges.map(salt =>
        builder.withRanges(
          if(builder.startRow.nonEmpty) Some(salt._1.get + builder.startRow.get) else salt._1,
          if(builder.stopRow.nonEmpty) Some(salt._1.get + builder.stopRow.get) else salt._2,
          Iterable.empty
        )
    )
  }

}
