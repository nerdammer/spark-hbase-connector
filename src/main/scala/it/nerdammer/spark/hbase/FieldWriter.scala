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

  implicit def longWriter: FieldWriter[Long] = new FieldWriter[Long] {
    override def map(data: Long): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  implicit def shortWriter: FieldWriter[Short] = new FieldWriter[Short] {
    override def map(data: Short): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  implicit def doubleWriter: FieldWriter[Double] = new FieldWriter[Double] {
    override def map(data: Double): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  implicit def floatWriter: FieldWriter[Float] = new FieldWriter[Float] {
    override def map(data: Float): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  implicit def booleanWriter: FieldWriter[Boolean] = new FieldWriter[Boolean] {
    override def map(data: Boolean): HBaseColumns = Seq(Bytes.toBytes(data))
  }

  implicit def bigDecimalWriter: FieldWriter[BigDecimal] = new FieldWriter[BigDecimal] {
    override def map(data: BigDecimal): HBaseColumns = Seq(Bytes.toBytes(data.bigDecimal))
  }

  implicit def stringWriter: FieldWriter[String] = new FieldWriter[String] {
    override def map(data: String): HBaseColumns = Seq(Bytes.toBytes(data))
  }
  
  // Tuples


  implicit def tupleWriter2[T1, T2](implicit c1: FieldWriter[T1], c2: FieldWriter[T2]): FieldWriter[(T1, T2)] = new FieldWriter[(T1, T2)] {
    override def map(data: (T1, T2)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2)
  }

  implicit def tupleWriter3[T1, T2, T3](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3]): FieldWriter[(T1, T2, T3)] = new FieldWriter[(T1, T2, T3)] {
    override def map(data: (T1, T2, T3)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3)
  }

  implicit def tupleWriter4[T1, T2, T3, T4](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4]): FieldWriter[(T1, T2, T3, T4)] = new FieldWriter[(T1, T2, T3, T4)] {
    override def map(data: (T1, T2, T3, T4)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4)
  }

  implicit def tupleWriter5[T1, T2, T3, T4, T5](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4], c5: FieldWriter[T5]): FieldWriter[(T1, T2, T3, T4, T5)] = new FieldWriter[(T1, T2, T3, T4, T5)] {
    override def map(data: (T1, T2, T3, T4, T5)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4) ++ c5.map(data._5)
  }

  implicit def tupleWriter6[T1, T2, T3, T4, T5, T6](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4], c5: FieldWriter[T5], c6: FieldWriter[T6]): FieldWriter[(T1, T2, T3, T4, T5, T6)] = new FieldWriter[(T1, T2, T3, T4, T5, T6)] {
    override def map(data: (T1, T2, T3, T4, T5, T6)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4) ++ c5.map(data._5) ++ c6.map(data._6)
  }

  implicit def tupleWriter7[T1, T2, T3, T4, T5, T6, T7](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4], c5: FieldWriter[T5], c6: FieldWriter[T6], c7: FieldWriter[T7]): FieldWriter[(T1, T2, T3, T4, T5, T6, T7)] = new FieldWriter[(T1, T2, T3, T4, T5, T6, T7)] {
    override def map(data: (T1, T2, T3, T4, T5, T6, T7)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4) ++ c5.map(data._5) ++ c6.map(data._6) ++ c7.map(data._7)
  }

  implicit def tupleWriter8[T1, T2, T3, T4, T5, T6, T7, T8](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4], c5: FieldWriter[T5], c6: FieldWriter[T6], c7: FieldWriter[T7], c8: FieldWriter[T8]): FieldWriter[(T1, T2, T3, T4, T5, T6, T7, T8)] = new FieldWriter[(T1, T2, T3, T4, T5, T6, T7, T8)] {
    override def map(data: (T1, T2, T3, T4, T5, T6, T7, T8)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4) ++ c5.map(data._5) ++ c6.map(data._6) ++ c7.map(data._7) ++ c8.map(data._8)
  }

  implicit def tupleWriter9[T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4], c5: FieldWriter[T5], c6: FieldWriter[T6], c7: FieldWriter[T7], c8: FieldWriter[T8], c9: FieldWriter[T9]): FieldWriter[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] = new FieldWriter[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
    override def map(data: (T1, T2, T3, T4, T5, T6, T7, T8, T9)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4) ++ c5.map(data._5) ++ c6.map(data._6) ++ c7.map(data._7) ++ c8.map(data._8) ++ c9.map(data._9)
  }

  implicit def tupleWriter10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](implicit c1: FieldWriter[T1], c2: FieldWriter[T2], c3: FieldWriter[T3], c4: FieldWriter[T4], c5: FieldWriter[T5], c6: FieldWriter[T6], c7: FieldWriter[T7], c8: FieldWriter[T8], c9: FieldWriter[T9], c10: FieldWriter[T10]): FieldWriter[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] = new FieldWriter[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] {
    override def map(data: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)): HBaseColumns = c1.map(data._1) ++ c2.map(data._2) ++ c3.map(data._3) ++ c4.map(data._4) ++ c5.map(data._5) ++ c6.map(data._6) ++ c7.map(data._7) ++ c8.map(data._8) ++ c9.map(data._9) ++ c10.map(data._10)
  }

}