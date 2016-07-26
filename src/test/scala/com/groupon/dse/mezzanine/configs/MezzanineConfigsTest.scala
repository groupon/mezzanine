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
import com.groupon.dse.mezzanine.util.SparkContextSetup
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FlatSpec

import scala.concurrent.duration.Duration

class MezzanineConfigsTest extends FlatSpec with SparkContextSetup {

  it should "return a MezzanineConfigs instance with the default values" in {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val config = MezzanineConfigs(new Properties(), sc.hadoopConfiguration)
    assert(config.stagingRootDir === new Path(MezzanineConfigs.StagingRootDir._2))
    assert(config.outputRootDir === new Path(MezzanineConfigs.OutputRootDir._2))
    assert(config.blockSize === fs.getDefaultBlockSize)
    assert(config.stagingExpiryDuration === Duration(15, TimeUnit.MINUTES))
    assert(config.retryInterval === Duration(5, TimeUnit.SECONDS))

    fs.close()
  }

  it should "return a MezzanineConfigs instance with the correct values" in {
    val properties = new Properties()
    val stagingRoot = "/staging/root/dir"
    val outputRoot = "/output/root/dir"
    val blockSize = 1024
    val stagingExpiryMinutes = 10
    val retryIntervalSeconds = 10
    properties.setProperty(MezzanineConfigs.StagingRootDir._1, stagingRoot)
    properties.setProperty(MezzanineConfigs.OutputRootDir._1, outputRoot)
    properties.setProperty(MezzanineConfigs.BlockSize._1, blockSize.toString)
    properties.setProperty(MezzanineConfigs.StagingExpiryMinutes._1, stagingExpiryMinutes.toString)
    properties.setProperty(MezzanineConfigs.HDFSRetryIntervalSeconds._1, retryIntervalSeconds.toString)

    val config = MezzanineConfigs(properties, sc.hadoopConfiguration)
    assert(config.stagingRootDir === new Path(stagingRoot))
    assert(config.outputRootDir === new Path(outputRoot))
    assert(config.blockSize === blockSize)
    assert(config.stagingExpiryDuration === Duration(stagingExpiryMinutes, TimeUnit.MINUTES))
    assert(config.retryInterval === Duration(retryIntervalSeconds, TimeUnit.SECONDS))
  }
}
