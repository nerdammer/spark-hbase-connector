package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Created by Nicola Ferraro on 17/01/15.
 */
class HBaseSimpleRDD[R: ClassTag](hadoopHBase: NewHadoopRDD[ImmutableBytesWritable, Result], builder: HBaseReaderBuilder[R])
                       (implicit mapper: FieldReader[R]) extends RDD[R](hadoopHBase) {

  override def getPartitions: Array[Partition] = firstParent[(ImmutableBytesWritable, Result)].partitions

  override def compute(split: Partition, context: TaskContext) = {
    // val cleanConversion = sc.clean ---> next version
    firstParent[(ImmutableBytesWritable, Result)].iterator(split, context)
      .map(e => conversion(e._1, e._2))
  }

  def conversion(key: ImmutableBytesWritable, row: Result) = {

    val columns = builder.columnsWithFamily
      .map(t => (Bytes.toBytes(t._1), Bytes.toBytes(t._2)))
      .map(t => if(row.containsColumn(t._1, t._2)) Some(CellUtil.cloneValue(row.getColumnLatestCell(t._1, t._2)).array) else None)

    mapper.map(new HBaseDataHolder(Bytes.toString(key.get), columns))
  }
}
