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

import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * [[Partitioner]] determines the output directory the data should be written to using a key to partition the data with.
 */
trait Partitioner {
  /**
   * @return root [[Path]] for staging data
   */
  def stagingRoot: Path

  /**
   * @return root [[Path]] for output data
   */
  def outputRoot: Path

  /**
   * Given a key, return the full output path that data for this key should be written to
   *
   * @param key key used in the staging directory to partition the data with
   * @return output [[Path]] for this key
   */
  def outputDirectory(key: String): Path

  /**
   * Glob pattern that will match all the leaf directories returned by `stagingDirectory`, where the unprocessed
   * data files are written to
   */
  lazy val stagingDirectoryGlob: Path = new Path(stagingRoot, s"${Partitioner.KeyPrefix}*")

  /**
   * Given a key, return the absolute path to the staging subdirectory for the data with that key
   *
   * @param key key to partition the staging data by
   * @return [[Path]] to the staging subdirectory for all the data with this key
   */
  def stagingDirectory(key: String): Path = new Path(stagingRoot, relativeStagingDirectory(key))

  /**
   * Given a path to a staging subdirectory, return the key used for that subdirectory name. This path must be of the
   * form /stagingRoot/key=`key`
   *
   * @param path staging subdirectory that has the key as part of its name
   * @return [[String]] for the key of this staging subdirectory
   */
  def keyForStagingDirectory(path: Path): String = path.getName.stripPrefix(s"${Partitioner.KeyPrefix}")

  /**
   * Given a key, return the relative path string to the staging subdirectory for the data with that key.
   *
   * This is relative to the staging root directory, so the absolute path will be `stagingRoot`/key=`topic`/.
   *
   * @param key key to partition the staging data by
   * @return [[String]] that represents the relative path to the staging subdirectory
   */
  def relativeStagingDirectory(key: String): String = s"${Partitioner.KeyPrefix}$key/"

  /**
   * Return all subdirectories under the staging root directory. Each subdirectory will have unprocessed data files for
   * that key.
   *
   * Calling this will return a [[Path]] collection that looks something like:
   * `[/staging/root/key=A, /staging/root/key=B, /staging/root/key=C]`
   *
   * @param fs [[FileSystem]] of where the staging data is stored
   * @return collection of [[Path]] instances, where each [[Path]] corresponds to a key
   */
  def stagingLeafDirectories(fs: FileSystem): Seq[Path] = {
    fs.globStatus(stagingDirectoryGlob).collect({
      // `getPathWithoutSchemeAndAuthority` removes the `file:` prefix on these paths that are actually directories
      case fileStatus if fileStatus.isDirectory => Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath)
    })
  }
}

object Partitioner {
  val KeyPrefix = "key="
}
