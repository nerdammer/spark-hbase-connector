package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.util.Bytes


/**
 * Created by Nicola Ferraro on 21/01/15.
 */

trait FieldWriter[T] extends Serializable {
  type HBaseColumns = Iterable[Array[Byte]]

  def map(data: T): HBaseColumns
}

trait FieldWriterConversions extends Serializable {

  implicit def intWriter: FieldWriter[Int] = new FieldWriter[Int] {
    override def map(data: Int): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  implicit def stringWriter: FieldWriter[String] = new FieldWriter[String] {
    override def map(data: String): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  // TEMP
  implicit def tupleWriter(implicit c: FieldWriter[String]): FieldWriter[(String, String)] = new FieldWriter[(String, String)] {
    override def map(data: (String, String)): HBaseColumns = c.map(data._1) ++ c.map(data._2)
  }

}