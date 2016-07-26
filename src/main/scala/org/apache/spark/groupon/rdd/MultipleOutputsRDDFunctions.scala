/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.groupon.rdd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.DataInputBuffer
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce.counters.GenericCounter
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter
import org.apache.hadoop.mapreduce.task.{ReduceContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptID, TaskType}
import org.apache.hadoop.util.Progress
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{SerializableWritable, TaskContext}

import scala.reflect.ClassTag

/**
 * Extra functions available on RDDs of the format (HDFS path string, (key, value)).
 *
 * This leverages Hadoop's [[MultipleOutputs]] API to write records in a single RDD different locations using the
 * specified path for each (key, value) pair.
 *
 * Most code here is taken directly from Spark's PairRDDFunctions:
 * https://github.com/apache/spark/blob/v1.5.2/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L992-L1057
 *
 * @param self RDD to
 * @param kt [[ClassTag]] for [[K]]. Used to get the runtime class for the keys.
 * @param vt [[ClassTag]] for [[V]]. Used to get the runtime class for the values.
 * @param ord [[Ordering]] for [[K]]. Null if not specified.
 * @tparam K type of the data keys
 * @tparam V type of the data values
 */
class MultipleOutputsRDDFunctions[K, V](self: RDD[(String, (K, V))])
                                      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Serializable {


  /**
   * Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop
   * Configuration object for that storage system. The hadoopConf should set an OutputFormat and any
   * output paths required (e.g. a table name to write to) in the same way as it would be
   * configured for a Hadoop MapReduce job.
   */
  def saveAsNewAPIHadoopDataset(hadoopConf: Configuration): Unit = self.withScope {
    val job = new Job(hadoopConf)
    val formatter = new SimpleDateFormat(MultipleOutputsRDDFunctions.DateFormat)
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (hadoopConf.getBoolean(MultipleOutputsRDDFunctions.ConfigValidateOutputSpecs, true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard = (context: TaskContext, itr: Iterator[(String, (K, V))]) => {
      val config = wrappedConf.value
      val attemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.REDUCE, context.partitionId(), context.attemptNumber())
      val hadoopContext = new TaskAttemptContextImpl(config, attemptId)
      val format = outfmt.newInstance
      format match {
        case c: Configurable => c.setConf(config)
        case _ => ()
      }
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

      val recordWriter = format.getRecordWriter(hadoopContext).asInstanceOf[RecordWriter[K, V]]
      val taskInputOutputContext = new ReduceContextImpl(
        config,
        attemptId,
        new DummyRawKeyValueIterator(itr),
        new GenericCounter,
        new GenericCounter,
        recordWriter,
        committer,
        new DummyReporter,
        null,
        kt.runtimeClass,
        vt.runtimeClass
      )
      val writer = new MultipleOutputs(taskInputOutputContext)

      var recordsWritten = 0L
      Utils.tryWithSafeFinally {
        while (itr.hasNext) {
          val (path, (key, value)) = itr.next()
          writer.write(key, value, path)

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } {
        writer.close()
      }

      committer.commitTask(hadoopContext)
      bytesWrittenCallback.foreach({ fn => outputMetrics.setBytesWritten(fn())})
      outputMetrics.setRecordsWritten(recordsWritten)
      1
    } : Int

    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.MAP, 0, 0)
    val jobTaskContext = new TaskAttemptContextImpl(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    self.context.runJob(self, writeShard)
    jobCommitter.commitJob(jobTaskContext)
  }

  /**
   * Dummy [[RawKeyValueIterator]] implementation, required to instantiate a [[ReduceContextImpl]], which is needed to
   * create a [[MultipleOutputs]] instance.
   *
   * This iterator itself is actually never accessed in the code path for Mezzanine's write operations. A
   * [[RawKeyValueIterator]] is used in a MapReduce job on the Reducer to iterate over key-value pairs of intermediate
   * data during the sort/merge step. However, in the context of Mezzanine, there is no concept of a sort/merge
   * step like this, and the `write` method of the [[MultipleOutputs]] class used here doesn't use this
   * [[RawKeyValueIterator]] at all.
   *
   * @param itr
   */
  private class DummyRawKeyValueIterator(itr: Iterator[_]) extends RawKeyValueIterator {
    override def getKey: DataInputBuffer = null
    override def getValue: DataInputBuffer = null
    override def getProgress: Progress = null
    override def next: Boolean = itr.hasNext
    override def close(): Unit = { }
  }

  /**
   * Initialize Hadoop metrics for this [[TaskContext]]
   *
   * @param context [[TaskContext]] of the write job being performed
   * @return the [[OutputMetrics]] instance to use and an optional function that fetches the amount of bytes written
   */
  private def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, Option[() => Long]) = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    if (bytesWrittenCallback.isDefined) {
      context.taskMetrics().outputMetrics = Some(outputMetrics)
    }
    (outputMetrics, bytesWrittenCallback)
  }

  /**
   * Records metrics for bytes written every [[MultipleOutputsRDDFunctions.RecordsBetweenBytesWrittenMetricUpdates]]
   * records
   *
   * @param bytesWrittenCallback optional function that fetches the amount of bytes written
   * @param outputMetrics instance of [[OutputMetrics]] that records metrics for this job
   * @param recordsWritten total number of records written so far
   */
  private def maybeUpdateOutputMetrics(bytesWrittenCallback: Option[() => Long],
                                       outputMetrics: OutputMetrics,
                                       recordsWritten: Long): Unit = {
    if (recordsWritten % MultipleOutputsRDDFunctions.RecordsBetweenBytesWrittenMetricUpdates == 0) {
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }
  }
}

object MultipleOutputsRDDFunctions {
  val DateFormat = "yyyyMMddHHmm"
  val ConfigValidateOutputSpecs = "spark.hadoop.validateOutputSpecs"
  val RecordsBetweenBytesWrittenMetricUpdates = 256
}
