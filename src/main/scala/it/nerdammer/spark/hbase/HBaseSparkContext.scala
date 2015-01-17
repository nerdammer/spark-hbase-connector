package it.nerdammer.spark.hbase

import it.nerdammer.spark.hbase.FieldMapper
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
 * Created by Nicola Ferraro on 10/01/15.
 */
class HBaseSparkContext(@transient sc: SparkContext) extends Serializable {

  def hbaseTable[R: ClassTag](table: String)(implicit mapper: FieldMapper[R]): HBaseReaderBuilder[R] = new HBaseReaderBuilder[R](sc, table=table)

}
