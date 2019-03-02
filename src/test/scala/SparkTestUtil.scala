import java.lang.annotation.{Annotation, Retention, RetentionPolicy}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.junit.After

import scala.collection.mutable

class SparkTestUtil[T](seq: Seq[T])(implicit val fun: (DStream[T], Option[String], Option[String], Option[String]) => DStream[T]) extends After {

  lazy val ssc = new StreamingContext("local", "test", Seconds(1))
  val rdd = ssc.sparkContext.makeRDD[T](seq)

  val stream = new ConstantInputDStream(ssc, rdd)

  val country = None
  val state = None
  val city = None

  val collector = mutable.MutableList[T]()

  fun(stream, country, state, city).foreachRDD(rdd => collector ++= rdd.collect())

  ssc.start()
  ssc.awaitTerminationOrTimeout(1000)

  def after = ssc.stop()

  override def annotationType(): Class[_ <: Annotation] = Class[SparkTestUtil]

}
