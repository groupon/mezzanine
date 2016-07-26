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

import java.nio.file.Files

import com.groupon.dse.mezzanine.outputformat.UncheckedTextOutputFormat
import com.groupon.dse.mezzanine.util.SparkContextSetup
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat, LazyOutputFormat}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class MultipleOutputsRDDFunctionsTest extends FlatSpec with SparkContextSetup with BeforeAndAfter {

  val testRoot: Path = new Path(Files.createTempDirectory("multiple-outputs-rdd-test").toString)
  val numTestFiles = 5

  var fs: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    fs = FileSystem.get(sc.hadoopConfiguration)
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

  def createWriteJob(): Job = {
    val job = Job.getInstance(sc.hadoopConfiguration)

    LazyOutputFormat.setOutputFormatClass(job, classOf[UncheckedTextOutputFormat[NullWritable, Text]])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    job.getConfiguration.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    FileOutputFormat.setOutputPath(job, testRoot)

    job
  }

  it should "write data in an RDD[(path, (key, value)] to the specified path for each (key, value) tuple" in {
    val data = for (message <- 'A' to 'E') yield message

    val rdd = sc.parallelize(data).map(message => {
      val directory = s"key=$message${Path.SEPARATOR}"
      (directory, (NullWritable.get(), new Text(message.toString)))
    })

    new MultipleOutputsRDDFunctions(rdd).saveAsNewAPIHadoopDataset(createWriteJob().getConfiguration)

    // Data for each message should be written to a separate directory
    val outputDirectories = fs.listStatus(testRoot)
    assert(outputDirectories.length === data.length)

    // Each directory should have a single file for a message
    outputDirectories.foreach(fileStatus => {
      val filesForKey = fs.listStatus(fileStatus.getPath)
      assert(filesForKey.length === 1)
    })
  }
}
