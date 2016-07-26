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

package com.groupon.dse.mezzanine.converter

import com.groupon.dse.mezzanine.util.SparkContextSetup
import org.apache.hadoop.io.NullWritable
import org.scalatest.FlatSpec

class NullAndTextWritableConverterTest extends FlatSpec with SparkContextSetup with WritableConverterTestSetup {

  it should "convert a RDD[TopicAndEvent] to a RDD[NullWritable, Text]" in {
    val converter = new NullAndTextWritableConverter
    val convertedData = sc.parallelize(topicAndEventData).map(topicAndEvent => {
      converter.convert(topicAndEvent)
    }).map({
      // Need to `map` here since the Hadoop `Writable` isn't serializable
      case (nullWritable, eventText) => (nullWritable.toString, eventText.toString)
    }).collect()

    assert(convertedData.length === topicAndEventData.length)
    convertedData.zip(topicAndEventData).foreach({
      case ((nullWritable, eventText), topicAndEvent) => {
        assert(nullWritable === NullWritable.get().toString)
        assert(eventText === topicAndEvent.event)
      }
    })
  }
}
