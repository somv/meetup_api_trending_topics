package com.humblefreak.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time}
import play.api.libs.json.{JsArray, Json}

object RealTimeProcessing extends Commons {
  
  def main(args: Array[String]): Unit = {

    runStreaming(Some("us"), None, Some(""))

  }

  def runStreaming(groupCountry: Option[String], groupState: Option[String], groupCity: Option[String]): Unit = {

    val receiverStreams = (1 to numberOfReceivers).map(x => ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps")))

    val unifiedStream = ssc.union(receiverStreams)

    unifiedStream.foreachRDD((rdd, time) => writeData(rdd, time))

    val groupInfo = unifiedStream.transform{
      rdd =>
        rdd.map {
          x =>
            val resJson = Json.parse(x)
            val guests = (resJson \ "guests").asOpt[Int].getOrElse(0)
            val group_country = (resJson \ "group" \ "group_country").asOpt[String].getOrElse("").toLowerCase
            val group_state = (resJson \ "group" \ "group_state").asOpt[String].getOrElse("").toLowerCase
            val group_city = (resJson \ "group" \ "group_city").asOpt[String].getOrElse("").toLowerCase
            (resJson \ "group" \ "group_topics")
              .asOpt[JsArray]
              .getOrElse(JsArray())
              .value
              .map(x => (((x \ "topic_name").asOpt[String].getOrElse("").toLowerCase), guests+1, group_country, group_state, group_city))
        }
    }

    val flattenGroupInfo = groupInfo.flatMap(list => list)

    val state = groupState.getOrElse("").toLowerCase
    val city = groupCity.getOrElse("").toLowerCase
    val country = groupCountry.getOrElse("").toLowerCase

    var filtered = flattenGroupInfo.window(Seconds(30), Seconds(10))

    if(country.nonEmpty) filtered = filtered.filter(tuple => tuple._3.equals(country))
    if(state.nonEmpty) filtered = filtered.filter(tuple => tuple._4.equals(state))
    if(city.nonEmpty) filtered = filtered.filter(tuple => tuple._5.equals(city))

//    filtered.print()

    filtered.map(tuple => (tuple._1, tuple._2)).reduceByKey((a: Int, b: Int) => a+b).transform(rdd =>rdd.sortBy(_._2, false)).print()

    ssc.start()

    ssc.awaitTermination()
  }

  def writeData(rdd: RDD[String], time: Time): Unit = {
    rdd.saveAsTextFile(writeDataDirectory+s"/${time.milliseconds}")
  }

}
