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

import java.nio.file.Files
import java.util.concurrent.TimeUnit
import com.groupon.dse.mezzanine.configs.MezzanineConfigs
import com.groupon.dse.mezzanine.outputformat.{StagingFileOutputFormat, UncheckedTextOutputFormat}
import com.groupon.dse.mezzanine.partitioner.{TopicPartitioner, Partitioner}
import com.groupon.dse.mezzanine.util.SparkContextSetup
import org.apache.hadoop.fs.{PathFilter, Path, FileStatus, FileSystem}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, NullWritable, Text}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{StreamInputInfo, BatchInfo, StreamingListenerBatchCompleted}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.concurrent.duration.Duration

class CompactorTest extends FlatSpec with SparkContextSetup with BeforeAndAfter {
  type K = NullWritable
  type V = Text
  val TimestampZero = 0
  val testRoot: Path = new Path(Files.createTempDirectory("compactor-test").toString)
  val numTestFiles = 5

  var fs: FileSystem = _
  var testFile: FileStatus = _
  var currTestDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    fs = FileSystem.get(sc.hadoopConfiguration)
    val testFilePath = new Path(testRoot, "test-seq-file")
    val writer = SequenceFile.createWriter(
      fs.getConf,
      SeqWriter.file(new Path(testRoot, "test-seq-file")),
      SeqWriter.keyClass(classOf[K]),
      SeqWriter.valueClass(classOf[V])
    )
    writer.append(NullWritable.get(), new Text(Array.fill[Byte](1024)(0)))
    writer.close()
    testFile = fs.getFileStatus(testFilePath)
    UserMetricsSystem.initialize(sc)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    }
    finally {
      fs.delete(testRoot, true)
      fs.close()
    }
  }

  before {
    // Populate current test directory with files
    currTestDir = new Path(Files.createTempDirectory("compactor-test").toString)
    (0 until numTestFiles).foreach(i => {
      val path = new Path(currTestDir, s"$i${StagingFileOutputFormat.FileExtension}")
      fs.copyFromLocalFile(testFile.getPath, path)
    })
  }

  after {
    fs.delete(currTestDir, true)
    currTestDir = null
  }

  def buildCompactor(blockSize: Long = testFile.getLen,
                     outputFormatClass: Class[_ <: FileOutputFormat[K, V]] = classOf[UncheckedTextOutputFormat[K, V]],
                     codec: Option[Class[_ <: CompressionCodec]] = None): Compactor[K, V] = {
    val configs = MezzanineConfigs(testRoot, testRoot, blockSize, Duration(15, TimeUnit.MINUTES), Duration.Zero)
    new Compactor[K, V](sc, fs, configs, outputFormatClass, codec)
  }


  "getFilesToCompact" should "return the all files to compact" in {
    val compactor = buildCompactor(blockSize = testFile.getLen)
    val testFiles = fs.listStatus(currTestDir).map(_.getPath)
    val filesToCompact = compactor.getFilesToCompact(currTestDir, System.currentTimeMillis())

    assert(filesToCompact.size === testFiles.length)
  }

  "filterFilesToCompact" should "return all files to compact" in {
    val compactor = buildCompactor(blockSize = testFile.getLen)
    val testFiles = fs.listStatus(currTestDir)
    val filesToCompact = compactor.filterFilesToCompact(testFiles, TimestampZero)

    assert(filesToCompact.size === testFiles.length)
  }

  "filterFilesToCompact with large maximum block size" should "return all files to compact" in {
    val compactor = buildCompactor(blockSize = testFile.getLen * numTestFiles)
    val testFiles = fs.listStatus(currTestDir)
    val filesToCompact = compactor.filterFilesToCompact(testFiles, TimestampZero)

    assert(filesToCompact.size === testFiles.length)
  }

  "filterFilesToCompact with large block size" should "return all files except one to compact" in {
    val compactor = buildCompactor(blockSize = testFile.getLen * (numTestFiles - 1))
    val testFiles = fs.listStatus(currTestDir)
    val filesToCompact = compactor.filterFilesToCompact(testFiles, TimestampZero)

    assert(filesToCompact.size === numTestFiles - 1)
  }
  
  "filterFilesToCompact with medium block size" should "return all files except one to compact" in {
    val compactor = buildCompactor(blockSize = testFile.getLen * 2)
    val testFiles = fs.listStatus(currTestDir)
    val filesToCompact = compactor.filterFilesToCompact(testFiles, TimestampZero)

    assert(filesToCompact.size === numTestFiles - 1)
  }

  "filterFilesToCompact" should "return all files when they are expired in the staging directory" in {
    val compactor = buildCompactor(blockSize = Long.MaxValue)
    val testFiles = fs.listStatus(currTestDir)
    val filesToCompact = compactor.filterFilesToCompact(testFiles, System.currentTimeMillis())

    assert(filesToCompact.size === numTestFiles)
  }

  "filterFilesToCompact" should "return only expired files when the rest of the files don't fill up a block size" in {
    val compactor = buildCompactor(blockSize = Long.MaxValue)
    // Roundabout way to set the modified time of these files the fs.setTimes API doesn't seem to set the modified
    // times correctly
    val testFiles = fs.listStatus(currTestDir).map(fileStatus => {
      val newPath = Path.mergePaths(fileStatus.getPath, new Path(".copy"))
      fs.copyFromLocalFile(true, fileStatus.getPath, newPath)
      Thread.sleep(1000)
      fs.getFileStatus(newPath)
    })

    val numOldFiles = 2
    val expireTimestampMillis = testFiles.sortBy(fileStatus => {
      fileStatus.getModificationTime
    }).take(numOldFiles).last.getModificationTime
    val filesToCompact = compactor.filterFilesToCompact(testFiles, expireTimestampMillis)

    assert(filesToCompact.size === numOldFiles)
  }

  "filterFilesToCompact" should "return all files if the combination of expired and new files fill up a block size" in {
    val compactor = buildCompactor(blockSize = testFile.getLen * numTestFiles)
    // Roundabout way to set the modified time of these files the fs.setTimes API doesn't seem to set the modified
    // times correctly
    val testFiles = fs.listStatus(currTestDir).map(fileStatus => {
      val newPath = Path.mergePaths(fileStatus.getPath, new Path(".copy"))
      fs.copyFromLocalFile(true, fileStatus.getPath, newPath)
      Thread.sleep(1000)
      fs.getFileStatus(newPath)
    })

    val expireTimestampMillis = testFiles.sortBy(fileStatus => {
      fileStatus.getModificationTime
    }).head.getModificationTime
    val filesToCompact = compactor.filterFilesToCompact(testFiles, expireTimestampMillis)

    assert(filesToCompact.size === numTestFiles)
  }

  "saveToOutputDir" should "merge and write data in files to a single output file" in {
    val compactor = buildCompactor()
    val testFiles = fs.listStatus(currTestDir)
    val outputDir = new Path(Files.createTempDirectory("compactor-test-output").toString)
    compactor.saveToOutputDir(testFiles.map(_.getPath), outputDir)
    val outputFiles = fs.listStatus(outputDir, new SparkOutputFilter)

    assert(outputFiles.length === 1)

    fs.delete(outputDir, true)
  }

  "pathsToString" should "return file names as a comma-separated string" in {
    val compactor = buildCompactor()
    val testFilePaths = fs.listStatus(currTestDir).map(_.getPath)
    val pathString = compactor.pathsToString(testFilePaths)

    assert(pathString.split(",").map(new Path(_)).toSet === testFilePaths.toSet)
  }

  "CompactorListener" should "move and compact staging files to a single output file" in {
    // Set up directories/files
    val stagingRoot = new Path(Files.createTempDirectory("compactor-listener-test-staging").toString)
    val outputRoot = new Path(Files.createTempDirectory("compactor-listener-test-output").toString)
    val topic = "test-topic"
    val topicStagingDir = new Path(stagingRoot, new Path(s"${Partitioner.KeyPrefix}$topic"))
    fs.mkdirs(topicStagingDir)
    fs.listStatus(currTestDir).foreach(fileStatus => {
      fs.copyFromLocalFile(fileStatus.getPath, new Path(topicStagingDir, fileStatus.getPath.getName))
    })

    // Build the listener and the batch completed event
    val configs = new MezzanineConfigs(
      stagingRoot, outputRoot, testFile.getLen, Duration(15, TimeUnit.MINUTES), Duration.Zero)
    val compactor = new Compactor[K, V](sc, fs, configs, classOf[UncheckedTextOutputFormat[K, V]])
    val compactorListener = new CompactorListener[K, V](
      sc, fs, new TopicPartitioner(stagingRoot, outputRoot), compactor
    )
    val currTime = System.currentTimeMillis()
    val batchCompletedEvent = new StreamingListenerBatchCompleted(new BatchInfo(
      new Time(currTime),
      Map[Int, StreamInputInfo]().empty,
      currTime,
      Some(currTime),
      Some(currTime))
    )

    compactorListener.onBatchCompleted(batchCompletedEvent)

    assert(fs.listStatus(topicStagingDir).length === 0)
    assert(fs.listStatus(outputRoot, new SparkOutputFilter).length === 1)

    fs.delete(stagingRoot, true)
    fs.delete(outputRoot, true)
  }
}

/**
 * Path filter to ignore the _SUCCESS file written by Spark/Hadoop after writing to the filesystem
 */
class SparkOutputFilter extends PathFilter {
  override def accept(path: Path): Boolean = {
    path.getName != "_SUCCESS"
  }
}
