package it.nerdammer.spark

import org.apache.spark.SparkContext

/**
 * Created by Nicola Ferraro on 10/01/15.
 */
package object hbase
                  extends HBaseSparkContextConversions
                  with FieldMapperConversions
                  with HBaseReaderBuilderConversions
