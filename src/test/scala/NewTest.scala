import com.humblefreak.analysis.{Commons, GroupData, RealTimeProcessing}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable

class NewTest extends FunSuite with BeforeAndAfter with Commons {

  @transient private var ssc: StreamingContext = _
  @transient private var sc: SparkContext = _

  val conf: Config = ConfigFactory.load

  val blockInterval: Int = conf.getInt("sparkStreaming.realTimeProcessingTestSuite.blockInterval")
  val batchInterval: Int = conf.getInt("sparkStreaming.realTimeProcessingTestSuite.batchInterval")
  val checkpointDirectory: String = conf.getString("sparkStreaming.realTimeProcessingTestSuite.checkpointDirectory")
  val appName: String = conf.getString("sparkStreaming.realTimeProcessing.appName")
  val master: String = conf.getString("sparkStreaming.realTimeProcessingTestSuite.master")
  val logLevel: String = conf.getString("sparkStreaming.realTimeProcessingTestSuite.logLevel")

  before {
    ssc = getSparkStreamingContext(appName, master, blockInterval, batchInterval,  checkpointDirectory, logLevel)
    sc = ssc.sparkContext
  }

  test("random test") {

    val inputData: mutable.Queue[RDD[GroupData]] = mutable.Queue()
    val inputStream: InputDStream[GroupData] = ssc.queueStream(inputData)

    val grp1 = GroupData("hiking", 1, "us", "TX", "Austin")
    val grp2 = GroupData("adventure", 1, "us", "TX", "Austin")

    inputData += sc.makeRDD[GroupData](List(grp1, grp2))


    inputStream.print()

    RealTimeProcessing.applyFilter(inputStream, None, None, None).print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
  }

  after {
    ssc.stop(true, true)
  }

}
