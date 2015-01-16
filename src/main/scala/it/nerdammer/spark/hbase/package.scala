package it.nerdammer.spark

import org.apache.spark.SparkContext

/**
 * Created by Nicola Ferraro on 10/01/15.
 */
package object hbase extends FieldMapperImplicits {

  // Include new methods into the SparkContext object
  implicit def toHBaseSparkContext(sc: SparkContext): HBaseSparkContext = new HBaseSparkContext(sc)

}

