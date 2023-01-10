package org.manypets.cam
package config

import iservice.TConfig
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Constants

/**
  * Class for implementing Spark configurations
  *
  * @author Gaurhari
  *
  * */
object SparkConfig extends TConfig {

  private val batchDuration = Seconds(Constants.batchDuration)

  private lazy val conf: SparkConf =
    new SparkConf()
  //.set("spark.streaming.backpressure.enabled", "true")
  /*.set(
        "fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName
      )*/

  private lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(Constants.sparkAppName)
    .config(conf)
    .master(Constants.sparkMasterUrl)
    .getOrCreate()

  def getSparkSession = {
    sparkSession.sparkContext.setLogLevel("OFF")
    sparkSession
  }

  lazy val sparkContext: SparkContext = sparkSession.sparkContext

  lazy val sqlContext: SQLContext = sparkSession.sqlContext

  lazy val streamingContext: StreamingContext = {
    val streamingContext = new StreamingContext(sparkContext, batchDuration)
    streamingContext.checkpoint(Constants.checkPointDir)
    streamingContext
  }

}
