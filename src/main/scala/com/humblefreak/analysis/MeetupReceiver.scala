package com.humblefreak.analysis

import java.io._

import com.ning.http.client._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MeetupReceiver (url: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  @transient var client: AsyncHttpClient = _

  @transient var inputPipe: PipedInputStream = _
  @transient var outputPipe: PipedOutputStream = _

  def onStart() {
    val cf = new AsyncHttpClientConfig.Builder()
    cf.setRequestTimeout(Integer.MAX_VALUE)
    cf.setReadTimeout(Integer.MAX_VALUE)
    cf.setPooledConnectionIdleTimeout(Integer.MAX_VALUE)
    client= new AsyncHttpClient(cf.build())

    inputPipe = new PipedInputStream(1024 * 1024)
    outputPipe = new PipedOutputStream(inputPipe)
    val producerThread = new Thread(new DataConsumer(inputPipe))
    producerThread.start()

    client.prepareGet(url).execute(new AsyncHandler[Unit]{

      def onBodyPartReceived(bodyPart: HttpResponseBodyPart) = {
        bodyPart.writeTo(outputPipe)
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
    if (Option(outputPipe).isDefined) {
      outputPipe.flush()
      outputPipe.close()
    }
    if (Option(inputPipe).isDefined) {
      inputPipe.close()
    }
  }

  class DataConsumer(inputStream: InputStream) extends Runnable {

    override def run() {
      val bufferedReader = new BufferedReader( new InputStreamReader( inputStream ))
      var input=bufferedReader.readLine()
      while(input!=null){
        store(input)
        input=bufferedReader.readLine()
      }
    }

  }

}
