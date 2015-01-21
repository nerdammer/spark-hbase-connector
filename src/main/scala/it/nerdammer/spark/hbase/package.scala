package it.nerdammer.spark

/**
 * Created by Nicola Ferraro on 10/01/15.
 */
package object hbase
                  extends HBaseSparkContextConversions
                  with FieldReaderConversions
                  with FieldWriterConversions
                  with HBaseReaderBuilderConversions
                  with HBaseWriterBuilderConversions