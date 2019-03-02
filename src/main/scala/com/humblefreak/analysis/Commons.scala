package com.humblefreak.analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming._

abstract class Commons {

  val blockInterval = 2

  val batchInterval = 10

  val checkpointDirectory = "/Users/b0205051/personal/ing_case_checkpoint_directory"

  val sparkConf = new SparkConf().setAppName("ing_case_analysis").setMaster("local[*]").set("spark.streaming.blockInterval", blockInterval.toString)

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val ssc = new StreamingContext(sc, Seconds(batchInterval))

  ssc.sparkContext.setLogLevel("ERROR")

  ssc.checkpoint(checkpointDirectory)

  val writeDataDirectory = "/Users/b0205051/personal/meetup_data"

  val numberOfReceivers = 2

}
