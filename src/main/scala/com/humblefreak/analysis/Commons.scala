package com.humblefreak.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

abstract class Commons {

    def getSparkStreamingContext(appName: String, master: String, blockInterval: Int, batchInterval: Int, checkpointDirectory: String, logLevel: String): StreamingContext = {

      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster(master)
        .set("spark.streaming.blockInterval", blockInterval.toString)

      val spark = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate()

      val sc = spark.sparkContext
      sc.setLogLevel(logLevel)

      val ssc = new StreamingContext(sc, Seconds(batchInterval))
      ssc.checkpoint(checkpointDirectory)

      ssc
    }

}
