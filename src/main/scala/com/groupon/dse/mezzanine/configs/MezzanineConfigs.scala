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

package com.groupon.dse.mezzanine.configs

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration.Duration

/**
 * Configs needed to run Mezzanine
 */
case class MezzanineConfigs(stagingRootDir: Path,
                            outputRootDir: Path,
                            blockSize: Long,
                            stagingExpiryDuration: Duration,
                            retryInterval: Duration) extends Serializable

object MezzanineConfigs {
  val StagingRootDir = ("dir.root.staging", "/tmp/mezzanine/staging")
  val OutputRootDir = ("dir.root.output", "/tmp/mezzanine/output")
  val BlockSize = ("hdfs.block.size", "")
  val StagingExpiryMinutes = ("staging.expiry.minutes", "15")
  val HDFSRetryIntervalSeconds = ("hdfs.retry.interval.seconds", "5")

  /**
   * Create a [[MezzanineConfigs]] instance with [[Properties]] and Hadoop [[Configuration]] instances. Uses default
   * values specified in the [[MezzanineConfigs]] object for fields if they are not set in [[Properties]]
   *
   * @param properties the [[Properties]] to use
   * @param hadoopConf the Hadoop [[Configuration]] to use
   * @return a [[MezzanineConfigs]] instance
   */
  def apply(properties: Properties, hadoopConf: Configuration): MezzanineConfigs = {
    val fs = FileSystem.get(hadoopConf)

    val stagingRootDir = new Path(properties.getProperty(StagingRootDir._1, StagingRootDir._2))
    val outputRootDir = new Path(properties.getProperty(OutputRootDir._1, OutputRootDir._2))
    val blockSize = properties.getProperty(BlockSize._1, fs.getDefaultBlockSize.toString).toLong
    val stagingExpiryDuration = Duration(
      properties.getProperty(StagingExpiryMinutes._1, StagingExpiryMinutes._2).toLong,
      TimeUnit.MINUTES
    )
    val retryInterval = Duration(
      properties.getProperty(HDFSRetryIntervalSeconds._1, HDFSRetryIntervalSeconds._2).toLong,
      TimeUnit.SECONDS
    )

    fs.close()

    new MezzanineConfigs(stagingRootDir, outputRootDir, blockSize, stagingExpiryDuration, retryInterval)
  }
}
