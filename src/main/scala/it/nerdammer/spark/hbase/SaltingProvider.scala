package it.nerdammer.spark.hbase

import java.util

import it.nerdammer.spark.hbase.conversion.{SingleColumnFieldWriter, FieldWriter}

import scala.reflect.ClassTag
import scala.util.Random

trait SaltingProvider[T] extends Serializable{

  def salting: Array[T]

  def salt(rowKey: Array[Byte]): T

  protected def verify(implicit writer: FieldWriter[T]): Unit = {
    if(length==0)
      throw new IllegalArgumentException("Salting cannot have length 0")
  }

  def length(implicit writer: FieldWriter[T]): Int = {
    if(!writer.isInstanceOf[SingleColumnFieldWriter[T]]) {
      throw new IllegalArgumentException("Salting array must be composed of primitive types")
    }
    val singleColumnFieldWriter = writer.asInstanceOf[SingleColumnFieldWriter[T]]

    salting
      .map(s => singleColumnFieldWriter.mapColumn(s))
      .map(o => o.getOrElse(Array[Byte]()))
      .map(a => a.size)
      .foldLeft(None.asInstanceOf[Option[Int]])((size, saltSize) => {
        if (size.nonEmpty && size.get != saltSize)
          throw new IllegalArgumentException(s"You cannot use salts with different lengths: ${size.get} and $saltSize")
        Some(saltSize)
      })
      .get
  }

}

trait SaltingProviderFactory[T] extends Serializable {

  def getSaltingProvider(salting: Iterable[T]): SaltingProvider[T]

}

class RandomSaltingProvider[T: ClassTag](val salting: Array[T])(implicit writer: FieldWriter[T]) extends SaltingProvider[T] {

  verify(writer)

  def this(saltingIterable: Iterable[T])(implicit writer: FieldWriter[T]) = this(saltingIterable.toArray)

  def randomizer = new Random

  override def salt(rowKey: Array[Byte]): T = salting(randomizer.nextInt(salting.size))

}

class HashSaltingProvider[T: ClassTag](val salting: Array[T])(implicit writer: FieldWriter[T]) extends SaltingProvider[T] {

  verify(writer)

  def this(saltingIterable: Iterable[T])(implicit writer: FieldWriter[T]) = this(saltingIterable.toArray)

  def hash(rowKey: Array[Byte]) = util.Arrays.hashCode(rowKey)

  override def salt(rowKey: Array[Byte]): T = salting((hash(rowKey) & 0x7fffffff) % salting.size)

}



trait SaltingProviderConversions {

  implicit def defaultHaltingProviderFactory[T: ClassTag](implicit writer: FieldWriter[T]): SaltingProviderFactory[T] = new SaltingProviderFactory[T] {

    def getSaltingProvider(salting: Iterable[T]): SaltingProvider[T] = new HashSaltingProvider[T](salting)
  }

}
