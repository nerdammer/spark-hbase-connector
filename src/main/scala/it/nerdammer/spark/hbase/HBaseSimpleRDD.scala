package it.nerdammer.spark.hbase

import it.nerdammer.spark.hbase.conversion.FieldReader
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class HBaseSimpleRDD[R: ClassTag](hadoopHBase: NewHadoopRDD[ImmutableBytesWritable, Result], builder: HBaseReaderBuilder[R], saltingLength: Int = 0)
                       (implicit mapper: FieldReader[R], saltingProvider: SaltingProviderFactory[String]) extends RDD[R](hadoopHBase) {

  override def getPartitions: Array[Partition] = firstParent[(ImmutableBytesWritable, Result)].partitions

  override def compute(split: Partition, context: TaskContext) = {
    // val cleanConversion = sc.clean ---> next version
    firstParent[(ImmutableBytesWritable, Result)].iterator(split, context)
      .map(e => conversion(e._1, e._2))
  }

  def conversion(key: ImmutableBytesWritable, row: Result) = {

    val columnNames = HBaseUtils.chosenColumns(builder.columns, mapper.columns)

    val columnNamesFC = HBaseUtils.columnsWithFamily(builder.columnFamily, columnNames)

    val columns = columnNamesFC
      .map(t => (Bytes.toBytes(t._1), Bytes.toBytes(t._2)))
      .map(t => if(row.containsColumn(t._1, t._2)) Some(CellUtil.cloneValue(row.getColumnLatestCell(t._1, t._2)).array) else None)
      .toList

    mapper.map(Some(key.get.drop(saltingLength)) :: columns)
  }
}
