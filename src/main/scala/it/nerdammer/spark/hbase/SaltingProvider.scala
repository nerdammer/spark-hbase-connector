package it.nerdammer.spark.hbase

import scala.reflect.ClassTag
import scala.util.Random

trait SaltingProvider[T] extends Serializable{

  def nextSalting: T

}

trait SaltingProviderFactory[T] extends Serializable {

  def getSaltingProvider(salting: Array[T]): SaltingProvider[T]

  def getSaltingProvider(salting: Iterable[T]): SaltingProvider[T]

}

class RandomSaltingProvider[T: ClassTag](salting: Array[T]) extends SaltingProvider[T] {

  def this(saltingIterable: Iterable[T]) = this(saltingIterable.toArray)

  def randomizer = new Random

  override def nextSalting: T = salting(randomizer.nextInt(salting.size))

}

trait SaltingProviderConversions {

  implicit def randomSaltingProviderFactory[T: ClassTag]: SaltingProviderFactory[T] = new SaltingProviderFactory[T] {

    def getSaltingProvider(salting: Array[T]): SaltingProvider[T] = new RandomSaltingProvider[T](salting)

    def getSaltingProvider(salting: Iterable[T]): SaltingProvider[T] = new RandomSaltingProvider[T](salting)
  }

}
