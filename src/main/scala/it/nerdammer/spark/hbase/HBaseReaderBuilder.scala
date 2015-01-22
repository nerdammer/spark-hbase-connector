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
      table: String,
      columnFamily: Option[String] = None,
      columns: Iterable[String] = Seq.empty,
      startRow: Option[String] = None,
      stopRow: Option[String] = None,
      salting: Iterable[String] = Seq.empty
      )
      (implicit mapper: FieldReader[R]) extends Serializable {

    protected[hbase] def withRanges(startRow: Option[String], stopRow: Option[String], salting: Iterable[String]) = copy(startRow = startRow, stopRow = stopRow, salting = salting)

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

    private[hbase] def columnsWithFamily(): Iterable[(String, String)] = {
      columns.map(c => {
        if(c.contains(':')) (c.substring(0, c.indexOf(':')), c.substring(c.indexOf(':') + 1))
        else (columnFamily.get, c)
      })
    }
}


trait HBaseReaderBuilderConversions extends Serializable {

  implicit def toHBaseRDD[R: ClassTag](builder: HBaseReaderBuilder[R])(implicit mapper: FieldReader[R]): RDD[R] = {
    if(builder.salting.isEmpty) {
      toSimpleHBaseRDD(builder)
    } else {
      require(builder.startRow.nonEmpty || builder.stopRow.nonEmpty, "Salting can be used only together with startRow and/or stopRow")

      val builders = getSaltedBuilders(builder)


      val rddSeq = builders.map(bui => toSimpleHBaseRDD(bui).asInstanceOf[RDD[R]]).toSeq
      val sc = rddSeq.head.sparkContext
      new HBaseSaltedRDD[R](sc, rddSeq)
    }
  }

  def toSimpleHBaseRDD[R: ClassTag](builder: HBaseReaderBuilder[R])(implicit mapper: FieldReader[R]): HBaseSimpleRDD[R] = {
    val hbaseConfig = HBaseSparkConf.fromSparkConf(builder.sc.getConf).createHadoopBaseConfig()

    hbaseConfig.set(TableInputFormat.INPUT_TABLE, builder.table)

    val columns =
      if(builder.columnFamily.isEmpty) builder.columns
      else builder.columns map (c => {
        if(c.indexOf(':') >= 0) c
        else builder.columnFamily.get + ':' + c
      })

    if(columns.exists(c => c.indexOf(':') < 0))
      throw new IllegalArgumentException("You must specify the default column family or use the fully qualified name of the columns. Eg. 'cf1:col1'")

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

    new HBaseSimpleRDD[R](rdd, builder)
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
