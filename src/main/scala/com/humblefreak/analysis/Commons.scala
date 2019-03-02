package com.humblefreak.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

abstract class Commons {

    val conf: Config = ConfigFactory.load

    val blockInterval: Int = conf.getInt("sparkStreaming.blockInterval")
    val batchInterval: Int = conf.getInt("sparkStreaming.batchInterval")
    val checkpointDirectory: String = conf.getString("sparkStreaming.checkpointDirectory")
    val writeDataDirectory: String = conf.getString("sparkStreaming.writeDataDirectory")
    val numberOfReceivers: Int = conf.getInt("sparkStreaming.numberOfReceivers")
    val apiURL: String = conf.getString("sparkStreaming.apiURL")
    val appName: String = conf.getString("sparkStreaming.appName")
    val master: String = conf.getString("sparkStreaming.master")
    val logLevel: String = conf.getString("sparkStreaming.logLevel")
    val windowDurationInSeconds: Int = conf.getInt("sparkStreaming.windowDurationInSeconds")
    val slideIntervalInSeconds: Int = conf.getInt("sparkStreaming.slideIntervalInSeconds")

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.streaming.blockInterval", blockInterval.toString)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    val sqlContext = spark.sqlContext

    val ssc = new StreamingContext(sc, Seconds(batchInterval))

    ssc.sparkContext.setLogLevel(logLevel)

    ssc.checkpoint(checkpointDirectory)

}
