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

package com.groupon.dse.mezzanine.outputformat

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

/**
 * Trait for [[FileOutputFormat]] with a custom skips the check for an existing directory.
 *
 * This trait seems like it would make more sense as just an abstract class, but it's kept as a trait because we want to
 * use it as a mixin with other FileOutputFormat implementations that come with Hadoop
 *
 * @tparam K type of the data keys
 * @tparam V type of the data values
 */
trait UncheckedFileOutputFormat[K, V] extends FileOutputFormat[K, V] {
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set")
    }

    // Get the delegation token for output directories file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array(outDir), job.getConfiguration)

    // Skipping directory check here
  }

  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val committer: FileOutputCommitter = getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
    new Path(committer.getWorkPath, uniqueFileName(context, extension))
  }

  /**
   * Generate a unique filename, using the task ID and TaskAttemptContext info passed in.
   *
   * @param context [[TaskAttemptContext]] of this write operation
   * @return a unique filename for this write operation
   */
  def uniqueFileName(context: TaskAttemptContext, extension: String = ""): String = {
    // getOutputName can return a directory String if used in conjunction with MultipleOutputs
    val pathPrefix = UncheckedFileOutputFormat.getOutputName(context) match {
      case directory if directory.endsWith(Path.SEPARATOR) => {
        directory + FileOutputFormat.PART
      }
      case filePrefix => {
        filePrefix
      }
    }

    val taskID = context.getTaskAttemptID.getTaskID.toString.replace("task_", "")

    s"$pathPrefix-$taskID$extension"
  }
}

object UncheckedFileOutputFormat {
  /**
   * Get the base output name for the output file.
   *
   * Re-implements FileOutputFormat.getOutputName. This is necessary because there is a limitation in Scala where it
   * cannot access `protected static` members in Java classes. See http://www.scala-lang.org/old/faq/4#4n1381 for
   * details
   *
   * @param job [[JobContext]] for this output operation
   * @return base output name for the output file.
   */
  def getOutputName(job: JobContext): String = {
    job.getConfiguration.get(FileOutputFormat.BASE_OUTPUT_NAME, FileOutputFormat.PART)
  }
}
