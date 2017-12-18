package com.chetan.spark.hbasehivepoc

/**
  * Created by chetan on 28/1/17.
  */
import it.nerdammer.spark.hbase._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import com.typesafe.config._
import org.slf4j.LoggerFactory
object SparkHBaseTransferHiveJob {
  val APP_NAME: String = "spark-hbase-job"
  val logger = LoggerFactory.getLogger(getClass.getName)
  var CONFIG_ENV: Config = null
  var HBASE_DB_HOST: Option[String] = None
  var HBASE_TABLE: Option[String] = None
  var HBASE_COLUMN_FAMILY: Option[String] = None
  var HIVE_DATA_WAREHOUSE: Option[String] = None
  def main(args: Array[String]) = {

    // read configuration from config file
    CONFIG_ENV = ConfigFactory.load("config.conf")

    // Initializing HBASE Configuration variables
    HBASE_DB_HOST = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-db-host"))
    HBASE_TABLE = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table"))
    HBASE_COLUMN_FAMILY = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-column-family"))
    // Initializing Hive Metastore configuration
    HIVE_DATA_WAREHOUSE = Some(CONFIG_ENV.getString(s"${APP_NAME}.hive-data-warehouse"))
    
    // Enable Hive with Hive warehouse in SparkSession
    val spark = SparkSession.builder().appName(APP_NAME).config("spark.sql.warehouse.dir", HIVE_DATA_WAREHOUSE.get).enableHiveSupport().getOrCreate()

    //Configuring Hbase host with Spark/Hadoop Configuration
    spark.sparkContext.hadoopConfiguration.set("spark.hbase.host", HBASE_DB_HOST.get)
    // using spark implicits implementation
    import spark.implicits._

    // Read HBase Table
    val hBaseRDD = spark.sparkContext.hbaseTable[(Option[String], Option[String], Option[String], Option[String], Option[String])]("university").select("stid", "name","subject","grade","city").inColumnFamily("emp")
    // Iterate HBaseRDD and generate RDD[Row]
    val rowRDD = hBaseRDD.map(i => Row(i._1.get,i._2.get,i._3.get,i._4.get,i._5.get))
    
    // Create Schema Structure
    object empSchema {
      val stid = StructField("stid", StringType)
      val name = StructField("name", StringType)
      val subject = StructField("subject", StringType)
      val grade = StructField("grade", StringType)
      val city = StructField("city", StringType)
      val struct = StructType(Array(stid, name, subject, grade, city))
    }

    // Create DataFrame with rowRDD and Schema structure
    val stdDf = spark.sqlContext.createDataFrame(rowRDD,empSchema.struct)
    
    // Saving Dataframe to Hive Table Successfully.
    stdDf.write.mode("append").saveAsTable("employee")

  }
}
