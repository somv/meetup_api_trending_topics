package com.humblefreak.analysis

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Time}
import play.api.libs.json.{JsArray, Json}

object RealTimeProcessing extends Commons {

  val conf: Config = ConfigFactory.load

  val blockInterval: Int = conf.getInt("sparkStreaming.realTimeProcessing.blockInterval")
  val batchInterval: Int = conf.getInt("sparkStreaming.realTimeProcessing.batchInterval")
  val checkpointDirectory: String = conf.getString("sparkStreaming.realTimeProcessing.checkpointDirectory")
  val writeDataDirectory: String = conf.getString("sparkStreaming.realTimeProcessing.writeDataDirectory")
  val numberOfReceivers: Int = conf.getInt("sparkStreaming.realTimeProcessing.numberOfReceivers")
  val apiURL: String = conf.getString("sparkStreaming.realTimeProcessing.apiURL")
  val appName: String = conf.getString("sparkStreaming.realTimeProcessing.appName")
  val master: String = conf.getString("sparkStreaming.realTimeProcessing.master")
  val logLevel: String = conf.getString("sparkStreaming.realTimeProcessing.logLevel")
  val windowDurationInSeconds: Int = conf.getInt("sparkStreaming.realTimeProcessing.windowDurationInSeconds")
  val slideIntervalInSeconds: Int = conf.getInt("sparkStreaming.realTimeProcessing.slideIntervalInSeconds")

  val ssc = getSparkStreamingContext(appName, master, blockInterval, batchInterval, checkpointDirectory, logLevel)

  def main(args: Array[String]): Unit = {

    startStreaming(20, Some(""), None, Some(""))

  }

  /**
    * Streaming entry point function, takes filters as input.
    *
    * This function has multiple receivers which can receive data concurrently from the streaming API.
    *
    * Function also applies a window on the predefined duration and sliding interval.
    *
    * @param topN top n topics to be returned on the console
    * @param groupCountry country geographical filter
    * @param groupState state geographical filter
    * @param groupCity city geographical filter
    */
  def startStreaming(topN: Int, groupCountry: Option[String], groupState: Option[String], groupCity: Option[String]): Unit = {

    val receiverStreams = (1 to numberOfReceivers).map(x => ssc.receiverStream(new DataReceiver(apiURL)))

    val unifiedStream = ssc.union(receiverStreams)

    val groupInfo = makeGroupData(unifiedStream)

    val flattenedGroupInfo = groupInfo.flatMap(list => list)

    val windowedDStream = flattenedGroupInfo.window(Seconds(windowDurationInSeconds), Seconds(slideIntervalInSeconds))

    val filtered = applyFilter(windowedDStream, groupCountry, groupState, groupCity)

    val resultantDStream = filtered
      .map(groupData => (groupData.topic, groupData.guests))
      .reduceByKey((a: Int, b: Int) => a+b)
      .transform(rdd =>rdd.sortBy(_._2, false))
      .transform(rdd => rdd.keys)


    resultantDStream.print(topN)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * A handler function to modify raw DStream to the required DStream format for analysis.
    *
    * Applies 'makeGroupDataFromResString' on each RDD
    *
    * @param inputStream
    * @return DStream[ Seq[GroupData] ]
    */
  def makeGroupData(inputStream: DStream[String]): DStream[Seq[GroupData]] = inputStream.transform (rdd => rdd.map(makeGroupDataFromResString))

  /**
    * This function takes response string (received from Meetup API) as input,
    * parses it to JSON and extracts required fields with play-json library.
    *
    * fields extracted are number of guests the member is bringing,
    * country, state and city of the group,
    * and the topics of this group.
    *
    * It makes GroupData object for each topic and returns a list of such GroupData objects.
    *
    * In case of exception in parsing the response JSON
    * it will simply return an constant empty GroupData object which can be filtered from the calling function.
    *
    * @param responseString
    * @return Seq[GroupData]
    *
    */
  def makeGroupDataFromResString(responseString: String): Seq[GroupData] = {
    try {
      val resJson = Json.parse(responseString)
      val guests = (resJson \ "guests").asOpt[Int].getOrElse(0) + 1 // 1 added to include the member also in the count of interested people in this topic.
      val country = (resJson \ "group" \ "group_country").asOpt[String].getOrElse("").toLowerCase
      val state = (resJson \ "group" \ "group_state").asOpt[String].getOrElse("").toLowerCase
      val city = (resJson \ "group" \ "group_city").asOpt[String].getOrElse("").toLowerCase
      (resJson \ "group" \ "group_topics")
        .asOpt[JsArray]
        .getOrElse(JsArray())
        .value
        .map(x => GroupData(((x \ "topic_name").asOpt[String].getOrElse("").toLowerCase), guests, country, state, city))
    } catch {
      case e: Exception =>
        println(s"Exception occurred while parsing JSON. ${e.getMessage}")
        Seq(GroupData("", 0, "", "", ""))
    }
  }

  /**
    * Function to apply geographical filters on the windowed DStream data.
    * Available filters are of Country, State and City where the group belongs.
    * It will apply filters only when the filters are non empty.
    *
    * @param windowedDStream
    * @param groupCountry
    * @param groupState
    * @param groupCity
    * @return DStream[GroupData]
    */
  def applyFilter(windowedDStream: DStream[GroupData], groupCountry: Option[String], groupState: Option[String], groupCity: Option[String]): DStream[GroupData] = {
    val filterCountry = groupCountry.getOrElse("").toLowerCase
    val filterState = groupState.getOrElse("").toLowerCase
    val filterCity = groupCity.getOrElse("").toLowerCase

    var filtered = windowedDStream

    if(filterCountry.nonEmpty) filtered = filtered.filter(groupData => groupData.country.equals(filterCountry))
    if(filterState.nonEmpty) filtered = filtered.filter(groupData => groupData.country.equals(filterState))
    if(filterCity.nonEmpty) filtered = filtered.filter(groupData => groupData.country.equals(filterCity))

    filtered
  }

  /**
    * A utility function to write rdd data to disk,
    * RDDs will be stored in directories of their time.
    *
    * @param rdd
    * @param time
    */
  def writeData(rdd: RDD[String], time: Time): Unit = rdd.saveAsTextFile(writeDataDirectory+s"/${time.milliseconds}")

}
