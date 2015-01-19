package it.nerdammer.spark.hbase

import org.apache.hadoop.hbase.util.Bytes


/**
 * Created by Nicola Ferraro on 10/01/15.
 */
trait FieldMapper[T] extends Serializable {
  def map(data: HBaseDataHolder): T
}

trait SingleColumnFieldMapper[T] extends FieldMapper[T] {

  def map(data: HBaseDataHolder): T =
    if(data.columns.size!=1) throw new IllegalArgumentException(s"Unexpected number of columns: expected 1, returned ${data.columns.size}")
    else columnMap(data.columns.head)

  def columnMap(cols: Array[Byte]): T
}

trait TupleFieldMapper[T <: Product] extends FieldMapper[T] {

  val n: Int

  def map(data: HBaseDataHolder): T =
    if(data.columns.size==n)
      tupleMap(data)
    else if(data.columns.size==n-1)
      tupleMap(new HBaseDataHolder(data.rowKey, Bytes.toBytes(data.rowKey) :: data.columns.toList))
    else
      throw new IllegalArgumentException(s"Unexpected number of columns: expected $n or ${n-1}, returned ${data.columns.size}")

  def tupleMap(data: HBaseDataHolder): T
}

trait FieldMapperConversions extends Serializable {

  // Simple types

  implicit def intMapper: FieldMapper[Int] = new SingleColumnFieldMapper[Int] {
    def columnMap(cols: Array[Byte]): Int = Bytes.toInt(cols)
  }

  implicit def longMapper: FieldMapper[Long] = new SingleColumnFieldMapper[Long] {
    def columnMap(cols: Array[Byte]): Long = Bytes.toLong(cols)
  }

  implicit def shortMapper: FieldMapper[Short] = new SingleColumnFieldMapper[Short] {
    def columnMap(cols: Array[Byte]): Short = Bytes.toShort(cols)
  }

  implicit def doubleMapper: FieldMapper[Double] = new SingleColumnFieldMapper[Double] {
    def columnMap(cols: Array[Byte]): Double = Bytes.toDouble(cols)
  }

  implicit def floatMapper: FieldMapper[Float] = new SingleColumnFieldMapper[Float] {
    def columnMap(cols: Array[Byte]): Float = Bytes.toFloat(cols)
  }

  implicit def booleanMapper: FieldMapper[Boolean] = new SingleColumnFieldMapper[Boolean] {
    def columnMap(cols: Array[Byte]): Boolean = Bytes.toBoolean(cols)
  }

  implicit def bigDecimalMapper: FieldMapper[BigDecimal] = new SingleColumnFieldMapper[BigDecimal] {
    def columnMap(cols: Array[Byte]): BigDecimal = Bytes.toBigDecimal(cols)
  }

  implicit def stringMapper: FieldMapper[String] = new SingleColumnFieldMapper[String] {
    def columnMap(cols: Array[Byte]): String = Bytes.toString(cols)
  }

  // Tuples

  implicit def tuple2Mapper[T1, T2](implicit m1: FieldMapper[T1], m2: FieldMapper[T2]): FieldMapper[(T1, T2)] = new TupleFieldMapper[(T1, T2)] {

    val n = 2

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      (m1.map(h1), m2.map(h2))
    }
  }

  implicit def tuple3Mapper[T1, T2, T3](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3]): FieldMapper[(T1, T2, T3)] = new TupleFieldMapper[(T1, T2, T3)] {

    val n = 3

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      (m1.map(h1), m2.map(h2), m3.map(h3))
    }
  }

  implicit def tuple4Mapper[T1, T2, T3, T4](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4]): FieldMapper[(T1, T2, T3, T4)] = new TupleFieldMapper[(T1, T2, T3, T4)] {

    val n = 4

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4))
    }
  }

  implicit def tuple5Mapper[T1, T2, T3, T4, T5](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4], m5: FieldMapper[T5]): FieldMapper[(T1, T2, T3, T4, T5)] = new TupleFieldMapper[(T1, T2, T3, T4, T5)] {

    val n = 5

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      val h5 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(4).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4), m5.map(h5))
    }
  }

  implicit def tuple6Mapper[T1, T2, T3, T4, T5, T6](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4], m5: FieldMapper[T5], m6: FieldMapper[T6]): FieldMapper[(T1, T2, T3, T4, T5, T6)] = new TupleFieldMapper[(T1, T2, T3, T4, T5, T6)] {

    val n = 6

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      val h5 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(4).head))
      val h6 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(5).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4), m5.map(h5), m6.map(h6))
    }
  }

  implicit def tuple7Mapper[T1, T2, T3, T4, T5, T6, T7](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4], m5: FieldMapper[T5], m6: FieldMapper[T6], m7: FieldMapper[T7]): FieldMapper[(T1, T2, T3, T4, T5, T6, T7)] = new TupleFieldMapper[(T1, T2, T3, T4, T5, T6, T7)] {

    val n = 7

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      val h5 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(4).head))
      val h6 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(5).head))
      val h7 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(6).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4), m5.map(h5), m6.map(h6), m7.map(h7))
    }
  }

  implicit def tuple8Mapper[T1, T2, T3, T4, T5, T6, T7, T8](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4], m5: FieldMapper[T5], m6: FieldMapper[T6], m7: FieldMapper[T7], m8: FieldMapper[T8]): FieldMapper[(T1, T2, T3, T4, T5, T6, T7, T8)] = new TupleFieldMapper[(T1, T2, T3, T4, T5, T6, T7, T8)] {

    val n = 8

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      val h5 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(4).head))
      val h6 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(5).head))
      val h7 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(6).head))
      val h8 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(7).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4), m5.map(h5), m6.map(h6), m7.map(h7), m8.map(h8))
    }
  }

  implicit def tuple9Mapper[T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4], m5: FieldMapper[T5], m6: FieldMapper[T6], m7: FieldMapper[T7], m8: FieldMapper[T8], m9: FieldMapper[T9]): FieldMapper[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] = new TupleFieldMapper[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {

    val n = 9

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      val h5 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(4).head))
      val h6 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(5).head))
      val h7 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(6).head))
      val h8 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(7).head))
      val h9 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(8).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4), m5.map(h5), m6.map(h6), m7.map(h7), m8.map(h8), m9.map(h9))
    }
  }

  implicit def tuple10Mapper[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](implicit m1: FieldMapper[T1], m2: FieldMapper[T2], m3: FieldMapper[T3], m4: FieldMapper[T4], m5: FieldMapper[T5], m6: FieldMapper[T6], m7: FieldMapper[T7], m8: FieldMapper[T8], m9: FieldMapper[T9], m10: FieldMapper[T10]): FieldMapper[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] = new TupleFieldMapper[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] {

    val n = 10

    def tupleMap(data: HBaseDataHolder) = {
      val h1 = new HBaseDataHolder(data.rowKey, Seq(data.columns.head))
      val h2 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(1).head))
      val h3 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(2).head))
      val h4 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(3).head))
      val h5 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(4).head))
      val h6 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(5).head))
      val h7 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(6).head))
      val h8 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(7).head))
      val h9 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(8).head))
      val h10 = new HBaseDataHolder(data.rowKey, Seq(data.columns.drop(9).head))
      (m1.map(h1), m2.map(h2), m3.map(h3), m4.map(h4), m5.map(h5), m6.map(h6), m7.map(h7), m8.map(h8), m9.map(h9), m10.map(h10))
    }
  }

}


