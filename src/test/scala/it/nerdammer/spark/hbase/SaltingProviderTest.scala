package it.nerdammer.spark.hbase

import java.util

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

class SaltingProviderTest extends FlatSpec with Matchers {



  "salting providers" should "throw exceptions if they are given an illegal salting" in {

    val goodSalting = Seq("1", "2", "3", "0")
    new RandomSaltingProvider(goodSalting)
    new HashSaltingProvider(goodSalting)

    val goodSaltingInt = Seq(1, 2, 3, 2223332)
    new RandomSaltingProvider(goodSaltingInt)
    new HashSaltingProvider(goodSaltingInt)

    val goodSaltingBoolean = Seq(true, false)
    new RandomSaltingProvider(goodSaltingBoolean)
    new HashSaltingProvider(goodSaltingBoolean)

    val wrongSalting = Seq("1", "2", "3", "09")
    assert(Try({new RandomSaltingProvider(wrongSalting)}).isFailure)
    assert(Try({new HashSaltingProvider(wrongSalting)}).isFailure)

    val wrongSaltingTuple = Seq(("1", "2"), ("3", "4"))
    assert(Try({new RandomSaltingProvider(wrongSaltingTuple)}).isFailure)
    assert(Try({new HashSaltingProvider(wrongSaltingTuple)}).isFailure)

  }

  "hash salting provider" should "not throw exceptions if the hashcode of the row is negative" in {

    val aSalting = Seq("1", "2", "3", "4")
    val sp: SaltingProvider[String] = new HashSaltingProvider(aSalting)

    val negativeHashArray = Array[Byte](1,-2,-3,-4, 5, 6, 7)
    // util.Arrays.hashCode = -1724796311

    assert(sp.salt(negativeHashArray)!=null)

  }




}
