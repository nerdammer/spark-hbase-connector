package it.nerdammer.spark.hbase.conversion

trait FieldMapper extends Serializable {

  type HBaseData = Iterable[Option[Array[Byte]]]

  def columns: Iterable[String] = Iterable.empty

}
