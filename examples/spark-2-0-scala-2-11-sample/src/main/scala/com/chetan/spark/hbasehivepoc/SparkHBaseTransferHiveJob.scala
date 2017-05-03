package com.chetan.spark.hbasehivepoc

/**
  * Created by chetan on 28/1/17.
  */
import it.nerdammer.spark.hbase._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
object SparkHBaseTransferHiveJob {
  val APP_NAME: String = "SparkHbaseJob"
  var HBASE_DB_HOST: String = null
  var HBASE_TABLE: String = null
  var HBASE_COLUMN_FAMILY: String = null
  var HIVE_DATA_WAREHOUSE: String = null
  def main(args: Array[String]) = {
    // Initializing HBASE Configuration variables
    HBASE_DB_HOST="127.0.0.1"
    HBASE_TABLE="university"
    HBASE_COLUMN_FAMILY="emp"
    // Initializing Hive Metastore configuration
    HIVE_DATA_WAREHOUSE = "/usr/local/hive/warehouse"
    // setting spark application
    val sparkConf = new SparkConf().setAppName(APP_NAME)
    //initialize the spark context
    val sparkContext = new SparkContext(sparkConf)
    //Configuring Hbase host with Spark/Hadoop Configuration
    sparkContext.hadoopConfiguration.set("spark.hbase.host", HBASE_DB_HOST)
    // Read HBase Table
    val hBaseRDD = sparkContext.hbaseTable[(Option[String], Option[String], Option[String], Option[String], Option[String])]("university").select("stid", "name","subject","grade","city").inColumnFamily("emp")
    // Iterate HBaseRDD and generate RDD[Row]
    val rowRDD = hBaseRDD.map(i => Row(i._1.get,i._2.get,i._3.get,i._4.get,i._5.get))
    // Create sqlContext for createDataFrame method
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    // Create Schema Structure
    object empSchema {
      val stid = StructField("stid", StringType)
      val name = StructField("name", StringType)
      val subject = StructField("subject", StringType)
      val grade = StructField("grade", StringType)
      val city = StructField("city", StringType)
      val struct = StructType(Array(stid, name, subject, grade, city))
    }

    import sqlContext.implicits._
    // Create DataFrame with rowRDD and Schema structure
    val stdDf = sqlContext.createDataFrame(rowRDD,empSchema.struct);
    // Enable Hive with Hive warehouse in SparkSession
    val spark = SparkSession.builder().appName(APP_NAME).config("spark.sql.warehouse.dir", HIVE_DATA_WAREHOUSE).enableHiveSupport().getOrCreate()
    // Importing spark implicits and sql package
    import spark.implicits._
    import spark.sql

    // Saving Dataframe to Hive Table Successfully.
    stdDf.write.mode("append").saveAsTable("employee")

  }
}
