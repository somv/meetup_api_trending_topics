import com.humblefreak.analysis.{GroupData, RealTimeProcessing}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SparkStreamingExampleSpec extends FunSuite with BeforeAndAfter with Eventually {

  private var sc:SparkContext = _
  private var ssc: StreamingContext = _


  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-streaming")

    ssc = new StreamingContext(sparkConf, Seconds(1))
    sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
  }

  test("dummy test") {
    val grpD1 = GroupData("hiking", 1, "us", "NY", "New York")
    val grpD2 = GroupData("adventure", 2, "us", "NY", "New York")
    val grpD3 = GroupData("adventure", 2, "us", "TX", "Austin")
    val seq = Seq(grpD1, grpD2, grpD3)

    val expectedOutput = Seq(grpD1, grpD2, grpD3)

    val rdd = ssc.sparkContext.makeRDD(seq)

    val stream = new ConstantInputDStream(ssc, rdd)

    stream.print()

    val collector = mutable.MutableList[GroupData]()

    var actualOutput: Seq[GroupData] = Seq()
    RealTimeProcessing.applyFilter(stream, None, None, None).foreachRDD(rdd => collector ++= rdd.collect())

    println(expectedOutput.toString())
    println(collector.toString())

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
  }

  after {
    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }

}
