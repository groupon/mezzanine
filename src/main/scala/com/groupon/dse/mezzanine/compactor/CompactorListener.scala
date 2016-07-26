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

package com.groupon.dse.mezzanine.compactor

import com.groupon.dse.mezzanine.partitioner.Partitioner
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Writable
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

/**
 * [[StreamingListener]] class that launches jobs to compact files written to the staging directory.
 *
 * @param sparkContext [[SparkContext]] used in the application
 * @param fs [[FileSystem]] where data is stored
 * @param partitioner [[Partitioner]] instance used to partition the staging data.
 * @param compactor [[Compactor]] used to write the output data
 * @tparam K type of the data keys
 * @tparam V type of the data values
 */
class CompactorListener[K <: Writable, V <: Writable](val sparkContext: SparkContext,
                                                      val fs: FileSystem,
                                                      val partitioner: Partitioner,
                                                      val compactor: Compactor[K, V]) extends StreamingListener {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    // Get the leaf directories where staging files are written to for the topics we consume
    val stagingLeafDirs = partitioner.stagingLeafDirectories(fs).filter(path => {
      path.getName.startsWith(Partitioner.KeyPrefix)
    })

    stagingLeafDirs.par.foreach(stagingPath => {
      val key = partitioner.keyForStagingDirectory(stagingPath)
      val filesToCompact = compactor.getFilesToCompact(stagingPath, batchCompleted.batchInfo.processingEndTime.get)
      if (filesToCompact.nonEmpty) {
        val outputPath = partitioner.outputDirectory(key)
        UserMetricsSystem.timer(s"mezzanine.write.time.output.$key").time({
          compactor.saveToOutputDir(filesToCompact, outputPath)
        })
        filesToCompact.foreach(fs.delete(_, false))
      }
    })
  }
}
