package com.humblefreak.analysis

import java.net._
import java.io._


import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class NewDataReceiver(url: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging with Serializable {

  @transient var urlForGetRequest: URL = null

  @transient var connection: HttpURLConnection =  null

  override def onStart(): Unit = {

    urlForGetRequest =  new URL(url)

    connection = urlForGetRequest.openConnection().asInstanceOf[HttpURLConnection]

    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()


  }

  override def onStop(): Unit = {
    if(connection != null) connection.disconnect()
  }

  def receive(): Unit = {

    connection.setRequestMethod("GET")
    val responseCode = connection.getResponseCode

    if(responseCode.equals(200)) {
      val in: BufferedReader  = new BufferedReader(new InputStreamReader(connection.getInputStream))
      var readLine: String = in.readLine()
      while (readLine != null) {
        System.out.println(readLine)
        store(readLine)
        readLine = in.readLine()
      }
      in.close()
    }

  }

}
