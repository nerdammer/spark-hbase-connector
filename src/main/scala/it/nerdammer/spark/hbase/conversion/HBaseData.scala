package it.nerdammer.spark.hbase.conversion

class HBaseData(val cells: Iterable[Option[Array[Byte]]], val names: Iterable[Option[String]] = Iterable.empty) {

}
