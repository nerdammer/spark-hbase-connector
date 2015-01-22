package it.nerdammer.spark.hbase

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.reflect.ClassTag

class HBaseSaltedRDD[R: ClassTag](sc: SparkContext, rdds: Seq[RDD[R]]) extends UnionRDD[R](sc, rdds) {

}
