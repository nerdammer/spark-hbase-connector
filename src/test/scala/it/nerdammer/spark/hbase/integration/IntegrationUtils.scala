package it.nerdammer.spark.hbase.integration

import it.nerdammer.spark.hbase.HBaseSparkConf
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.{SparkConf, SparkContext}

object IntegrationUtils extends Serializable {

  @transient lazy val sparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    new SparkContext(sparkConf)
  }

  def createTable(table: String, columnFamily: String) = {
    val conf = HBaseSparkConf()
    val admin = new HBaseAdmin(conf.createHadoopBaseConfig())

    val tableDesc = new HTableDescriptor(TableName.valueOf(table))
    tableDesc.addFamily(new HColumnDescriptor(columnFamily))
    admin.createTable(tableDesc)
  }

  def dropTable(table: String) = {
    val conf = HBaseSparkConf()
    val admin = new HBaseAdmin(conf.createHadoopBaseConfig())

    admin.disableTable(table)
    admin.deleteTable(table)
  }

  def pad(str: String, size: Int): String =
    if(str.length>=size) str
    else pad("0" + str, size)

}
