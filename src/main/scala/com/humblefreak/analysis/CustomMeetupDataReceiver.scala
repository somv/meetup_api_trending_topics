package com.humblefreak.analysis

import java.io._
import java.nio.charset.StandardCharsets

import com.ning.http.client._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomMeetupDataReceiver(url: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {


  @transient var client: AsyncHttpClient = null

  def onStart() {
    val cf = new AsyncHttpClientConfig.Builder()
    cf.setRequestTimeout(Integer.MAX_VALUE)
    cf.setReadTimeout(Integer.MAX_VALUE)
    cf.setPooledConnectionIdleTimeout(Integer.MAX_VALUE)
    client = new AsyncHttpClient(cf.build())

    client.prepareGet(url).execute(new AsyncHandler[Unit]{

      def onBodyPartReceived(bodyPart: HttpResponseBodyPart) = {

//        val bout: BufferedOutputStream = new BufferedOutputStream(fout)
        val bytes = bodyPart.getBodyPartBytes
        println(bytes.length)
        val str: String = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8)
        store(str)
//        println("=========="+str)
//        val is: InputStream = new ByteArrayInputStream(bytes)
//
//        val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(is))
//        bufferedReader
//        var input=bufferedReader.readLine()
//        while(input!=null){
//          store(input)
//          input=bufferedReader.readLine()
//        }
        AsyncHandler.STATE.CONTINUE
      }

      def onStatusReceived(status: HttpResponseStatus) = {
        AsyncHandler.STATE.CONTINUE
      }

      def onHeadersReceived(headers: HttpResponseHeaders) = {
        AsyncHandler.STATE.CONTINUE
      }

      def onCompleted = {
        println("completed")
      }


      def onThrowable(t: Throwable)={
        t.printStackTrace()
      }

    })

  }

  def onStop() {
    if (Option(client).isDefined) client.close()
  }

}
