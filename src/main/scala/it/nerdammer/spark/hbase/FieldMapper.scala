package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.util.Bytes


/**
 * Created by Nicola Ferraro on 10/01/15.
 */
trait FieldMapper[T] {
  def map(data: HBaseDataHolder): T
}

trait SingleColumnFieldMapper[T] extends FieldMapper[T] {

  def map(data: HBaseDataHolder): T =
    if(data.result.size!=1) throw new IllegalArgumentException(s"Unexpected number of columns: expected 1, returned ${data.result.size}")
    else columnMap(data.result.head)

  def columnMap(cols: Array[Byte]): T
}

trait FieldMapperImplicits {

  implicit def intMapper: FieldMapper[Int] = new SingleColumnFieldMapper[Int] {
    def columnMap(cols: Array[Byte]): Int = Bytes.toInt(cols)
  }

  implicit def stringMapper: FieldMapper[String] = new SingleColumnFieldMapper[String] {
    def columnMap(cols: Array[Byte]): String = Bytes.toString(cols)
  }


}


