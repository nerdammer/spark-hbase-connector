package it.nerdammer.spark

import it.nerdammer.spark.hbase.conversion.{FieldWriterConversions, FieldReaderConversions}

package object hbase
                  extends HBaseSparkContextConversions
                  with SaltingProviderConversions
                  with FieldReaderConversions
                  with FieldWriterConversions
                  with HBaseReaderBuilderConversions
                  with HBaseWriterBuilderConversions