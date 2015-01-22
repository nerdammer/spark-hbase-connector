package it.nerdammer.spark.hbase.integration

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Nicola Ferraro on 22/01/15.
 */
object IntegrationUtils {

  lazy val sparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setAppName("test")
    new SparkContext(sparkConf)
  }

}
