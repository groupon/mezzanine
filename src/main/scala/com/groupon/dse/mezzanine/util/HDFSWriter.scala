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

package com.groupon.dse.mezzanine.util

import com.groupon.dse.util.Utils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{LazyOutputFormat, FileOutputCommitter, FileOutputFormat}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.groupon.rdd.MultipleOutputsRDDFunctions
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * Utility class to handle the logic to write to HDFS.
 */
object HDFSWriter {
  val RetryIndefinitely = -1
  val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
  private lazy val retryMeter = UserMetricsSystem.meter("mezzanine.write.retry.rate")

  /**
   * Attempt to write to HDFS. If the write fails, keeps retrying until the write succeeds.
   *
   * @param rdd pair [[RDD]] that contains the data to write
   * @param writePath [[Path]] to write the data to
   * @param outputFormatClass class of the [[FileOutputFormat]] to use
   * @param retryInterval [[Duration]] of the time between retry attempts
   * @param codec optional [[CompressionCodec]] to use for this write
   * @param kt [[ClassTag]] for `K`. Used to get the runtime class for the keys.
   * @param vt [[ClassTag]] for `V`. Used to get the runtime class for the values.
   * @tparam K type of the data keys
   * @tparam V type of the data values
   */
  def writeToHDFSwithRetry[K <: Writable, V <: Writable](rdd: RDD[(K, V)],
                                                         writePath: Path,
                                                         outputFormatClass: Class[_ <: FileOutputFormat[K, V]],
                                                         retryInterval: Duration,
                                                         codec: Option[Class[_ <: CompressionCodec]] = None)
                                                        (implicit kt: ClassTag[K], vt: ClassTag[V]): Unit = {
    val job = Job.getInstance(rdd.sparkContext.hadoopConfiguration)
    setCommonConfigs[K, V](job, writePath, codec)
    job.setOutputFormatClass(outputFormatClass)

    performHDFSWrite(
      {
        new PairRDDFunctions(rdd).saveAsNewAPIHadoopDataset(job.getConfiguration)
      },
      retryInterval
    )
  }

  /**
   * Attempt to write to multiple paths on HDFS at the same time. If the write fails, keeps retrying until the write
   * succeeds.
   *
   * The input RDD must be of type (String, (K, V)). K and V represent the types of the data that is written to HDFS.
   * The first String element of the RDD tuple must be a valid directory on HDFS to write the (K, V) element to.
   *
   * @param rdd pair [[RDD]] that contains the data to write
   * @param writePath [[Path]] to write the data to
   * @param outputFormatClass class of the [[FileOutputFormat]] to use
   * @param retryInterval [[Duration]] of the time between retry attempts
   * @param codec optional [[CompressionCodec]] to use for this write
   * @param kt [[ClassTag]] for `K`. Used to get the runtime class for the keys.
   * @param vt [[ClassTag]] for `V`. Used to get the runtime class for the values.
   * @tparam K type of the data keys
   * @tparam V type of the data values
   */
  def multiWriteToHDFSwithRetry[K <: Writable, V <: Writable](rdd: RDD[(String, (K, V))],
                                                              writePath: Path,
                                                              outputFormatClass: Class[_ <: FileOutputFormat[K, V]],
                                                              retryInterval: Duration,
                                                              codec: Option[Class[_ <: CompressionCodec]] = None)
                                                              (implicit kt: ClassTag[K], vt: ClassTag[V]): Unit = {
    val job = Job.getInstance(rdd.sparkContext.hadoopConfiguration)
    setCommonConfigs[K, V](job, writePath, codec)
    LazyOutputFormat.setOutputFormatClass(job, outputFormatClass)

    performHDFSWrite(
      {
        new MultipleOutputsRDDFunctions[K, V](rdd).saveAsNewAPIHadoopDataset(job.getConfiguration)
      },
      retryInterval
    )
  }

  /**
   * Set common configs for any HDFS write operation in Mezzanine
   */
  private def setCommonConfigs[K <: Writable, V <: Writable](job: Job,
                                                             writePath: Path,
                                                             codec: Option[Class[_ <: CompressionCodec]])
                                                            (implicit kt: ClassTag[K], vt: ClassTag[V]): Unit = {
    job.setOutputKeyClass(kt.runtimeClass)
    job.setOutputValueClass(vt.runtimeClass)
    FileOutputFormat.setOutputPath(job, writePath)
    // Do not write _SUCCESS files when saving data, since multiple write operations can happen to the same directory
    job.getConfiguration.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)

    // Add compression params if required
    if (codec.nonEmpty) {
      job.getConfiguration.set("mapreduce.output.fileoutputformat.compress", "true")
      job.getConfiguration.set("mapreduce.output.fileoutputformat.compress.codec", codec.get.getCanonicalName)
      job.getConfiguration.set("mapreduce.output.fileoutputformat.compress.codec.type", "BLOCK")
    }
  }

  /**
   * Keeps attempting to write to HDFS until the write is successful
   *
   * @param writeJob closure with the write action
   * @param retryInterval [[Duration]] of the time between retry attempts
   */
  private def performHDFSWrite(writeJob: => Unit,
                               retryInterval: Duration): Unit = {
    Utils.doActionWithRetry(
      writeJob,
      {
        exception => {
          logger.error(s"Error writing to HDFS. Retrying in ${retryInterval.toSeconds} seconds.", exception)
          retryMeter.mark()
          Thread.sleep(retryInterval.toMillis)
        }
      },
      RetryIndefinitely
    )
  }
}
