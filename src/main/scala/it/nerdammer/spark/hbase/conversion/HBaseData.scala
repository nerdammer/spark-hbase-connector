package it.nerdammer.spark.hbase.conversion

class HBaseData(val cells: Iterable[Option[Array[Byte]]], val names: Iterable[Option[String]]) {

  def drop(n: Int): HBaseData = new HBaseData(cells.drop(n), names.drop(n))

  def head(): HBaseData = new HBaseData(Seq(cells.head), Seq(names.head))

  def ++(other: HBaseData): HBaseData = new HBaseData(cells ++ other.cells, names ++ other.names)

}
