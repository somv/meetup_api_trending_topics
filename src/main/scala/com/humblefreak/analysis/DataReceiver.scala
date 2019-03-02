package com.humblefreak.analysis

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client._

import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream

import org.apache.spark.internal.Logging

class DataReceiver(url: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  @transient var client: AsyncHttpClient = _
  @transient var inputPipe: PipedInputStream = _
  @transient var outputPipe: PipedOutputStream = _

  def onStart(): Unit = {
    val cf = new AsyncHttpClientConfig.Builder()
    cf.setRequestTimeout(Integer.MAX_VALUE)
    cf.setReadTimeout(Integer.MAX_VALUE)
    cf.setPooledConnectionIdleTimeout(Integer.MAX_VALUE)
    client= new AsyncHttpClient(cf.build())

    inputPipe = new PipedInputStream(100)
    outputPipe = new PipedOutputStream(inputPipe)
    val producerThread = new Thread(new DataConsumer(inputPipe))
    producerThread.start()

    val asyncHandler: AsyncHandler[Unit] = new AsyncHandler[Unit] {
      def onBodyPartReceived(bodyPart: HttpResponseBodyPart): AsyncHandler.STATE = {
        bodyPart.writeTo(outputPipe)
        AsyncHandler.STATE.CONTINUE
      }

      def onStatusReceived(status: HttpResponseStatus): AsyncHandler.STATE = {
        AsyncHandler.STATE.CONTINUE
      }

      def onHeadersReceived(headers: HttpResponseHeaders): AsyncHandler.STATE = {
        AsyncHandler.STATE.CONTINUE
      }

      def onCompleted: Unit = {
        println("Completed")
      }


      def onThrowable(t: Throwable): Unit = {
        t.printStackTrace()
      }
    }

    client.prepareGet(url).execute(asyncHandler)
  }

  def onStop(): Unit = {
    if (Option(client).isDefined) client.close()
    if (Option(outputPipe).isDefined) {
      outputPipe.flush()
      outputPipe.close()
    }
    if (Option(inputPipe).isDefined) {
      inputPipe.close()
    }
  }

  class DataConsumer(inputStream: InputStream) extends Runnable {

    override def run(): Unit = {
      val bufferedReader = new BufferedReader( new InputStreamReader( inputStream ))
      var input=bufferedReader.readLine()
      while(input!=null){
        store(input)
        input=bufferedReader.readLine()
      }
    }

  }

}
