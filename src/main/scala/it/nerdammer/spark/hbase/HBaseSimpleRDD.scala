package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{TaskContext, Partition}

import scala.collection.JavaConversions._

import scala.reflect.ClassTag

/**
 * Created by Nicola Ferraro on 17/01/15.
 */
class HBaseSimpleRDD[R: ClassTag](hadoopHBase: NewHadoopRDD[ImmutableBytesWritable, Result])
                       (implicit mapper: FieldMapper[R]) extends RDD[R](hadoopHBase) {

  override def getPartitions: Array[Partition] = firstParent[(ImmutableBytesWritable, Result)].partitions

  override def compute(split: Partition, context: TaskContext) = {
    // val cleanConversion = sc.clean ---> next version
    firstParent[(ImmutableBytesWritable, Result)].iterator(split, context)
      .map(e => conversion(e._1, e._2))
  }

  def conversion(key: ImmutableBytesWritable, row: Result) =
    mapper.map(new HBaseDataHolder((row.listCells.map(c => CellUtil.cloneValue(c).array))))


}
