package it.nerdammer.spark.hbase

import it.nerdammer.spark.hbase.conversion.FieldWriter
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class HBaseWriterBuilder[R: ClassTag] private[hbase] (
      private[hbase] val rdd: RDD[R],
      private[hbase] val table: String,
      private[hbase] val columnFamily: Option[String] = None,
      private[hbase] val columns: Iterable[String] = Seq.empty,
      private[hbase] val salting: Iterable[String] = Seq.empty
      )(implicit mapper: FieldWriter[R], saltingProvider: SaltingProviderFactory[String])
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

    def withSalting(salting: Iterable[String]) = {
      require(salting.size > 1, "Invalid salting. Two or more elements are required")
      require(this.salting.isEmpty, "Salting has already been set")

      this.copy(salting = salting)
    }

}

class HBaseWriterBuildable[R: ClassTag](rdd: RDD[R])(implicit mapper: FieldWriter[R], sal: SaltingProviderFactory[String]) extends Serializable {

  def toHBaseTable(table: String) = new HBaseWriterBuilder[R](rdd, table)

}

class HBaseWriter[R: ClassTag](builder: HBaseWriterBuilder[R])(implicit mapper: FieldWriter[R], saltingProviderFactory: SaltingProviderFactory[String]) extends Serializable {

  def save(): Unit = {

    val conf = HBaseSparkConf.fromSparkConf(builder.rdd.sparkContext.getConf).createHadoopBaseConfig()
    conf.set(TableOutputFormat.OUTPUT_TABLE, builder.table)

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])

    val saltingProvider: Option[SaltingProvider[String]] =
      if(builder.salting.isEmpty) None
      else Some(saltingProviderFactory.getSaltingProvider(builder.salting))

    val transRDD = builder.rdd.map(r => {

      val convertedData: Iterable[Option[Array[Byte]]] = mapper.map(r)

      if(convertedData.size<2) {
        throw new IllegalArgumentException("Expected at least two converted values, the first one should be the row key")
      }

      val columnsNames = HBaseUtils.chosenColumns(builder.columns, mapper.columns)

      val rawRowKey = convertedData.head.get
      val columns = convertedData.drop(1)

      if(columns.size!=columnsNames.size) {
        throw new IllegalArgumentException(s"Wrong number of columns. Expected ${columnsNames.size} found ${columns.size}")
      }

      val rowKey =
        if(saltingProvider.isEmpty) rawRowKey
        else Bytes.toBytes(saltingProvider.get.salt(rawRowKey) + Bytes.toString(rawRowKey))

      val put = new Put(rowKey)

      columnsNames.zip(columns).foreach {
        case (name, Some(value)) => {
          val family = if(name.contains(':')) Bytes.toBytes(name.substring(0, name.indexOf(':'))) else Bytes.toBytes(builder.columnFamily.get)
          val column = if(name.contains(':')) Bytes.toBytes(name.substring(name.indexOf(':') + 1)) else Bytes.toBytes(name)

          put.add(family, column, value)
        }
        case _ => {}
      }

      (new ImmutableBytesWritable, put)
    })

    transRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

}

trait HBaseWriterBuilderConversions extends Serializable {

  implicit def rddToHBaseBuilder[R: ClassTag](rdd: RDD[R])(implicit mapper: FieldWriter[R]): HBaseWriterBuildable[R] = new HBaseWriterBuildable[R](rdd)

  implicit def writerBuilderToWriter[R: ClassTag](builder: HBaseWriterBuilder[R])(implicit mapper: FieldWriter[R]): HBaseWriter[R] = new HBaseWriter[R](builder)

}
