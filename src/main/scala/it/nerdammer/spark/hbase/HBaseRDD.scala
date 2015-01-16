package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, Logging, SparkContext}
import org.apache.spark.rdd.{NewHadoopRDD, HadoopRDD, RDD}

import scala.collection.JavaConversions._

import scala.reflect.ClassTag


/**
 * Created by Nicola Ferraro on 10/01/15.
 */
class HBaseRDD[R: ClassTag] private[hbase] (@transient sc: SparkContext,
                      table: String,
                      columnFamily: Option[String] = None,
                      columns: Iterable[String] = Seq.empty,
                      startRow: Option[String] = None,
                      stopRow: Option[String] = None
                                                  )(implicit mapper: FieldMapper[R]) extends RDD[R](sc, Seq.empty) with Logging {

  private def copy(columnFamily: Option[String] = columnFamily,
                   columns: Iterable[String] = columns,
                   startRow: Option[String] = startRow,
                   stopRow: Option[String] = stopRow) = new HBaseRDD[R](sc, table, columnFamily, columns, startRow, stopRow)

  // Start of the Builder section

  def withColumnFamily(columnFamily: String) = this.copy(columnFamily = Some(columnFamily))

  def withColumns(columns: String*): HBaseRDD[R] = {require(columns.nonEmpty); this.copy(columns = columns)}

  def withStartRow(startRow: String) = this.copy(startRow = Some(startRow))

  def withStopRow(stopRow: String) = this.copy(startRow = Some(stopRow))

  // RDD functions

	@DeveloperApi
	override def compute(split: Partition, context: TaskContext): Iterator[R] = hadoopRDD.compute(split, context)

	override protected def getPartitions: Array[Partition] = Array.empty //hadoopRDD.publicGetPartitions

  // Underlying RDD

  lazy val hadoopRDD: RDD[R] = {
    val hbaseConfig = HBaseSparkConf.fromSparkConf(sc.getConf).createHadoopReadConfig(table)

    val rdd = sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])




    rdd.map {
      case (key, row) => {
        //val keyStr = Bytes.toString(key.get)
        val data = new HBaseDataHolder((row.listCells.map(c => CellUtil.cloneValue(c).array)))
        mapper.map(data)
      }
    }


  }


}