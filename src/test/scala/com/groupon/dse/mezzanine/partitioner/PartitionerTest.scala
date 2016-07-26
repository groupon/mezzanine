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

package com.groupon.dse.mezzanine.partitioner

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FlatSpec

class PartitionerTest extends FlatSpec with PartitionerTestSetup {
  val partitioner: Partitioner = new TopicPartitioner(stagingRoot, outputRoot)

  "stagingDirectory" should "return the staging directory partitioned by key" in {
    val stagingPath = partitioner.stagingDirectory(partitionerKey)

    assert(stagingPath === new Path(stagingRoot, s"${Partitioner.KeyPrefix}$partitionerKey") )
  }

  "outputDirectory" should "return the correct output directory given a key" in {
    val outputPath = partitioner.outputDirectory(partitionerKey)

    assert(outputPath === partitioner.outputDirectory(partitionerKey) )
  }

  "keyForStagingDirectory" should "return the key for a staging subdirectory" in {
    val stagingPath = partitioner.stagingDirectory(partitionerKey)

    assert(partitionerKey === partitioner.keyForStagingDirectory(stagingPath))
  }

  "stagingLeafDirectories" should "return only staging subdirectories that are partitioned by key" in {
    val fs = FileSystem.getLocal(new Configuration())
    val localStagingRoot = new Path(Files.createTempDirectory("partitioner-test").toString)
    val localPartitioner: Partitioner = new TopicPartitioner(localStagingRoot, outputRoot)

    // Create temporary directories/files for testing
    val validStagingDirs = for (i <- 0 to 3) yield new Path(localStagingRoot, s"${Partitioner.KeyPrefix}$i")
    validStagingDirs.foreach(fs.mkdirs)
    val invalidStagingDirs = for (i <- 0 to 3) yield new Path(localStagingRoot, s"$i")
    invalidStagingDirs.foreach(fs.mkdirs)
    val invalidStagingFiles = for (i <- 0 to 3) yield new Path(localStagingRoot, s"$i.tmp")
    invalidStagingFiles.foreach(fs.createNewFile)

    assert(localPartitioner.stagingLeafDirectories(fs).toSet === validStagingDirs.toSet)
    fs.delete(localStagingRoot, true)
    fs.close()
  }
}
