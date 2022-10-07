/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.kafka.source

import io.cdap.cdap.api.data.format.StructuredRecord
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.api.java.function.{Function2, VoidFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import java.util.function.Consumer

/**
 * DStream that implements {@link StreamingEventHandler} .
 * This DStream will keep the Kafka offsets for each batch RDD before applying the _transformFunction.
 * On calling onBatchCompleted, the _stateConsumer will be provided with these offsets.
 *
 * @param _ssc           Spark streaming context
 * @param _kafkaDStream  DStream created through KafkaUtil.createDirectStream
 * @param _kafkaConf     Config object for Kafka Streaming Source
 * @param _stateConsumer Consumer function for the state produced
 */
class KafkaDStream(_ssc: StreamingContext,
                   _kafkaDStream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]],
                   _transformFunction: Function2[ConsumerRecord[Array[Byte], Array[Byte]], Time, StructuredRecord],
                   _stateConsumer: VoidFunction[Array[OffsetRange]])
  extends DStream[StructuredRecord](_ssc) with StreamingEventHandler {

  // For keeping the offsets in each batch
  private var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

  override def slideDuration: Duration = _kafkaDStream.slideDuration

  override def dependencies: List[DStream[_]] = List(_kafkaDStream)

  override def compute(validTime: Time): Option[RDD[StructuredRecord]] = {
    val rddOption = _kafkaDStream.compute(validTime)
    val transformFn = _transformFunction;
    // If there is a RDD produced, cache the offsetRanges for the batch and then transform to RDD[StructuredRecord]
    rddOption.map(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(record => transformFn.call(record, validTime))
    })
  }

  override def onBatchCompleted(context: io.cdap.cdap.etl.api.streaming.StreamingContext): Unit = {
    _stateConsumer.call(offsetRanges)
  }

  /**
   * Convert this to a {@link JavaDStream}
   *
   * @return JavaDStream
   */
  def convertToJavaDStream(): JavaDStream[StructuredRecord] = {
    JavaDStream.fromDStream(this)
  }
}
