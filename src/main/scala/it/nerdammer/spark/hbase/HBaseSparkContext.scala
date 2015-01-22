package it.nerdammer.spark.hbase

import it.nerdammer.spark.hbase.conversion.FieldReader
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class HBaseSparkContext(@transient sc: SparkContext) extends Serializable {

  def hbaseTable[R: ClassTag](table: String)(implicit mapper: FieldReader[R]): HBaseReaderBuilder[R] = new HBaseReaderBuilder[R](sc, table=table)

}

trait HBaseSparkContextConversions extends Serializable {
  // Include new methods into the SparkContext object
  implicit def toHBaseSparkContext(sc: SparkContext): HBaseSparkContext = new HBaseSparkContext(sc)
}