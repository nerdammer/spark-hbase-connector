package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

/**
 * Created by Nicola Ferraro on 20/01/15.
 */
case class HBaseWriterBuilder[R: ClassTag] private[hbase] (
      rdd: RDD[R],
      table: String,
      columnFamily: Option[String] = None,
      columns: Iterable[String] = Seq.empty
      )(implicit m: FieldWriter[R])
      extends Serializable {


    def toColumns(columns: String*): HBaseWriterBuilder[R] = {
      require(this.columns.isEmpty, "Columns have already been set")
      require(columns.nonEmpty, "You should provide at least one column")

      this.copy(columns = columns)
    }

    def inColumnFamily(columnFamily: String) = {
      require(this.columnFamily.isEmpty, "Default column family has already been set")
      require(columnFamily.nonEmpty, "Invalid column family provided")

      this.copy(columnFamily = Some(columnFamily))
    }

}

class HBaseWriterBuildable[R: ClassTag](rdd: RDD[R])(implicit m: FieldWriter[R]) extends Serializable {

  def toHBaseTable(table: String) = new HBaseWriterBuilder[R](rdd, table)

}

class HBaseWriter[R: ClassTag](builder: HBaseWriterBuilder[R])(implicit writer: FieldWriter[R]) extends Serializable {

  def save(): Unit = {

    val conf = HBaseSparkConf.fromSparkConf(builder.rdd.sparkContext.getConf).createHadoopBaseConfig()
    conf.set(TableOutputFormat.OUTPUT_TABLE, builder.table)

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])


    val transRDD = builder.rdd.map(r => {
      val converted: Iterable[Array[Byte]] = writer.map(r)
      if(converted.size<2) {
        throw new IllegalArgumentException("Expected at least two converted values, the first one should be the row key")
      }
      val rowkey = converted.head
      val columns = converted.drop(1)

      if(columns.size!=builder.columns.size) {
        throw new IllegalArgumentException(s"Wrong number of columns. Expected ${builder.columns.size} found ${columns.size}")
      }

      val put = new Put(rowkey)

      builder.columns.zip(columns).foreach {
        case (name, value) => {
          val family = if(name.contains(':')) Bytes.toBytes(name.substring(0, name.indexOf(':'))) else Bytes.toBytes(builder.columnFamily.get)
          val column = if(name.contains(':')) Bytes.toBytes(name.substring(name.indexOf(':') + 1)) else Bytes.toBytes(name)

          put.add(family, column, value)
        }
      }

      (new ImmutableBytesWritable, put)
    })

    transRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

}

trait HBaseWriterBuilderConversions extends Serializable {

  implicit def rddToHBaseBuilder[R: ClassTag](rdd: RDD[R])(implicit m: FieldWriter[R]): HBaseWriterBuildable[R] = new HBaseWriterBuildable[R](rdd)

  implicit def writerBuilderToWriter[R: ClassTag](builder: HBaseWriterBuilder[R])(implicit m: FieldWriter[R]): HBaseWriter[R] = new HBaseWriter[R](builder)

}