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

import com.groupon.dse.mezzanine.configs.MezzanineConfigs
import com.groupon.dse.mezzanine.outputformat.StagingFilesFilter
import com.groupon.dse.mezzanine.util.HDFSWriter
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
 * Handles the compaction of many small files in the staging directory into a smaller number of large files for the
 * final output. The sizes of these output files should be as close to, but not greater than, a multiple of a block
 * size as possible.
 *
 * @param sparkContext [[SparkContext]] used in the application
 * @param fs [[FileSystem]] where data is stored
 * @param configs [[MezzanineConfigs]] used in the application
 * @param outputFormatClass class of the [[FileOutputFormat]] used to write the output data
 * @param codec optional [[CompressionCodec]] used for writing the output data
 * @tparam K type of the data keys
 * @tparam V type of the data values
 */
class Compactor[K <: Writable : ClassTag, V <: Writable : ClassTag](val sparkContext: SparkContext,
                                                                    val fs: FileSystem,
                                                                    val configs: MezzanineConfigs,
                                                                    val outputFormatClass: Class[_ <: FileOutputFormat[K, V]],
                                                                    val codec: Option[Class[_ <: CompressionCodec]] = None) {

  val stagingFilesFilter = new StagingFilesFilter

  /**
   * Takes a staging directory with data files ready for compaction and returns a subset of these files which should be
   * compacted into a single file.
   *
   * @param stagingPath directory that contains the unprocessed data. The data here should belong to a single
   *                    [[com.groupon.dse.mezzanine.partitioner.Partitioner]] key
   * @param compactUpToMillis max timestamp of files to consider compaction for. This is to prevent moving files that
   *                          executors may still be writing to.
   * @return [[Path]] collection that represents the files ready to be compacted
   */
  def getFilesToCompact(stagingPath: Path, compactUpToMillis: Long): Traversable[Path] = {
    // Get the staging data files in `stagingDir`
    val stagingFiles = fs.listStatus(stagingPath, stagingFilesFilter).filter(status =>
      status.isFile && status.getModificationTime < compactUpToMillis
    )

    // 'expireTimestampMillis' will be a timestamp rounded down to the nearest 'configs.stagingExpiryDuration', so that
    // expired staging files will be compacted every 'configs.stagingExpiryDuration' minutes
    val expiryDurationMillis = configs.stagingExpiryDuration.toMillis
    val expireTimestampMillis = (compactUpToMillis / expiryDurationMillis) * expiryDurationMillis

    // Filter the staging files to get only the ones we want to compact into a single file
    filterFilesToCompact(stagingFiles, expireTimestampMillis).map(_.getPath)
  }

  /**
   * Returns the list of files in a particular staging subdirectory that will be compacted together.
   *
   * This considers two things:
   *
   * The first is the timestamp of the files. Files older than `expireTimestampMillis` will be moved to the output
   * directory automatically, to ensure that data from low-volume topics will still be compacted frequently.
   *
   * The second is the the block size B of HDFS, so that the result is as close as possible to a multiple of the block
   * size n * B, but still less than n * B.
   *
   * @param files List of files that are considered for compaction
   * @param expireTimestampMillis Files older than this timestamp will automatically be moved to the output
   *                                      directory, regardless of the size of the output file
   * @return Subset of `files` that will be moved to the compaction directory.
   */
  def filterFilesToCompact(files: Traversable[FileStatus], expireTimestampMillis: Long): Traversable[FileStatus] = {
    // Files that have a timestamp older than 'expireTimestampMillis' should get moved to the output
    // directory, regardless of the file size
    val (expiredFiles, currentFiles) = files.partition(fileStatus => {
      fileStatus.getModificationTime <= expireTimestampMillis
    })

    // In addition to the files past the expiry threshold, select more data if the size of the output file will be close
    // to a block size
    val totalSize = files.foldLeft(0L)((total, fileStatus) => total + fileStatus.getLen)
    val maxSize = (totalSize / configs.blockSize) * configs.blockSize
    var outputSize = expiredFiles.foldLeft(0L)((total, fileStatus) => total + fileStatus.getLen)

    val currentFilesToCompact = currentFiles.takeWhile(fileStatus => {
      outputSize += fileStatus.getLen
      outputSize <= maxSize
    })

    expiredFiles ++ currentFilesToCompact
  }

  /**
   * Write to a single output file by the data in `filesToCompact`
   *
   * @param filesToCompact collection of files containing the data to compact into a single file
   * @param outputPath output [[Path]] to write to
   */
  def saveToOutputDir(filesToCompact: Traversable[Path], outputPath: Path): Unit = {
    // Read all files in compaction directory and repartition down to a single partition
    val rddToCompact = sparkContext.sequenceFile[K, V](pathsToString(filesToCompact)).coalesce(1)
    // Save the data as a single file
    HDFSWriter.writeToHDFSwithRetry(rddToCompact, outputPath, outputFormatClass, configs.retryInterval, codec)
  }

  /**
   * Takes a collection of paths and returns a comma-separated string of these paths
   *
   * @param paths a [[Traversable]] of paths
   * @return a comma-separated list of absolute paths
   */
  def pathsToString(paths: Traversable[Path]): String = paths.mkString(",")
}
