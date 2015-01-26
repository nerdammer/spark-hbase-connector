package it.nerdammer.spark.hbase

object HBaseUtils extends Serializable {

  def chosenColumns(colOptions: Iterable[String]*): Iterable[String] = {
    val valid = colOptions.filter(c => c.nonEmpty)
    if(valid.size==0) throw new IllegalArgumentException("No columns have been defined for the operation")
    if(valid.size>1) throw new IllegalArgumentException("Columns are defined twice, you must define them only once")
    valid.head
  }

  def columnsWithFamily(defaultColumnFamily: Option[String], columns: Iterable[String]): Iterable[(String, String)] =
    columns.map(c =>
      if(c.indexOf(':')>=0) (c.substring(0, c.indexOf(':')), c.substring(c.indexOf(':') + 1))
      else if(defaultColumnFamily.isEmpty) throw new IllegalArgumentException("Default column family is mandatory when column names are not fully qualified")
      else (defaultColumnFamily.get, c)
    )

  def fullyQualifiedColumns(defaultColumnFamily: Option[String], columns: Iterable[String]): Iterable[String] =
    columnsWithFamily(defaultColumnFamily, columns).map {case (cf, c) => cf + ":" + c}

}
