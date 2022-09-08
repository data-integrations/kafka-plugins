package io.cdap.plugin.source

import io.cdap.cdap.api.data.format.StructuredRecord
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


/**
 * Scala implementation of DStream
 *
 * @param _ssc          the Spark streaming context
 * @param _kafkaDStream the DStream created through KafkaUtil.createDirectStream
 *
 */
class KafkaDStream(_ssc: StreamingContext,
                   _kafkaDStream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]],
                   _kafkaConf: KafkaConfig)
  extends DStream[StructuredRecord](_ssc) with StreamingEventHandler {

  var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

  override def slideDuration: Duration = _kafkaDStream.slideDuration

  override def dependencies: List[DStream[_]] = List(_kafkaDStream)

  override def compute(validTime: Time): Option[RDD[StructuredRecord]] = {
    val rddOption = _kafkaDStream.compute(validTime)

    // If there is a RDD produced,
    // update the offsetRanges from the latest fetch from Kafka before
    // transforming to RDD[StructuredRecord] to return
    val transformFn = KafkaStreamingSourceUtil.getTransformFunction(_kafkaConf, validTime.milliseconds);
    rddOption.map(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(record => transformFn.call(record))
    })
  }

  override def onBatchCompleted(context: io.cdap.cdap.etl.api.streaming.StreamingContext): Unit = {
    if (offsetRanges.length > 0) {
      KafkaStreamingSourceUtil.print(offsetRanges)
    }
  }

  def convertToJavaDStream(): JavaDStream[StructuredRecord] = {
    JavaDStream.fromDStream(this)
  }
}

