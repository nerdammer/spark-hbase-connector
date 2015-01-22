package it.nerdammer.spark.hbase.conversion

class HBaseData(val rowKey: String, val columns: Iterable[Option[Array[Byte]]]) {

}
