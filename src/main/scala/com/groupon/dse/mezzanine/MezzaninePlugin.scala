/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.groupon.dse.mezzanine

import com.groupon.dse.kafka.common.WrappedMessage
import com.groupon.dse.mezzanine.converter.WritableConverter
import com.groupon.dse.mezzanine.extractor.{TopicAndEvent, WrappedMessageExtractor}
import com.groupon.dse.mezzanine.outputformat.StagingFileOutputFormat
import com.groupon.dse.mezzanine.partitioner.Partitioner
import com.groupon.dse.mezzanine.util.HDFSWriter
import com.groupon.dse.spark.plugins.ReceiverPlugin
import org.apache.hadoop.io.Writable
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * Baryon [[ReceiverPlugin]] class that reads a [[WrappedMessage]], extracts its contents, and partitions the data.
 *
 * When writing to the staging directory, the data is stored as a SequenceFile, so the types for the key and value need
 * to be specified.
 *
 * @param extractor [[WrappedMessageExtractor]] class used to read the [[WrappedMessage]] payload
 * @param converter [[WritableConverter]] class used to convert[[com.groupon.dse.mezzanine.extractor.TopicAndEvent]]
 *                 messages to Hadoop-compatible (key, value) tuples
 * @param partitioner [[Partitioner]] class used to partition the staging data by a certain key. In this case the key
 *                    we use is the Kafka topic. This needs to be annotated as @transient because Spark ends up
 *                    serializing the entire MezzaninePlugin class, and the [[Partitioner]] uses Hadoop's
 *                    [[org.apache.hadoop.fs.Path]] objects which aren't serializable. This annotation doesn't cause
 *                    any problems though, as the [[Partitioner]] is actually only used in the driver anyways.
 * @param retryInterval when a write to HDFS fails, the interval to retry the write.
 */
class MezzaninePlugin[K <: Writable : ClassTag, V <: Writable: ClassTag](extractor: WrappedMessageExtractor,
                                                                         converter: WritableConverter[TopicAndEvent, K, V],
                                                                         @transient partitioner: Partitioner,
                                                                         retryInterval: Duration) extends ReceiverPlugin {

  /**
   * Write the [[WrappedMessage]] data into the staging directory, partitioned by the key specified in the
   * [[Partitioner]] class.
   *
   * @param messages [[RDD]] of [[WrappedMessage]] to write to the staging directory.
   */
  override def execute(messages: RDD[WrappedMessage]): Unit = {
    val topics = messages.mapPartitions(partitionIterator => {
      if (partitionIterator.nonEmpty) {
        Iterator((partitionIterator.next().topic, null))
      } else {
        Iterator.empty
      }
    }).reduceByKeyLocally((x, y) => x).keys
    val topicToStagingPath = topics.map(topic => topic -> partitioner.relativeStagingDirectory(topic)).toMap

    val topicEventRDD = extractor.extract(messages)

    val pathEventRDD = topicEventRDD.mapPartitions(partitionIterator => {
      if (partitionIterator.nonEmpty) {
        val firstTopicAndEvent = partitionIterator.next()
        val stagingPath = topicToStagingPath.get(firstTopicAndEvent.topic).get

        (Iterator(firstTopicAndEvent) ++ partitionIterator).map(topicAndEvent => {
          (stagingPath, converter.convert(topicAndEvent))
        })
      } else {
        Iterator.empty
      }
    }).cache()

    HDFSWriter.multiWriteToHDFSwithRetry(
      pathEventRDD,
      partitioner.stagingRoot,
      classOf[StagingFileOutputFormat[K, V]],
      retryInterval
    )
  }
}
