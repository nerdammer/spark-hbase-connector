package com.user.integration

import it.nerdammer.spark.hbase.HBaseSparkConf
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}

object IntegrationUtils extends Serializable {

  @transient lazy val sparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    new SparkContext(sparkConf)
  }

  def createTable(table: String, columnFamily: String): Unit = createTable(table, Seq(columnFamily))


  def createTable(table: String, columnFamilies: Seq[String]): Unit = {
    val conf = HBaseSparkConf()
    val admin = new HBaseAdmin(conf.createHadoopBaseConfig())

    val tableDesc = new HTableDescriptor(TableName.valueOf(table))
    columnFamilies.foreach(cf => {
      tableDesc.addFamily(new HColumnDescriptor(cf))
    })

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
