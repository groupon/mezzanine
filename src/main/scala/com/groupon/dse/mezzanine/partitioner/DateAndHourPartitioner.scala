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

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path

/**
 * [[Partitioner]] that partitions data for each key by date and hour.
 *
 * @param stagingRoot root [[Path]] for staging data
 * @param outputRoot root [[Path]] for output data
 */
class DateAndHourPartitioner(override val stagingRoot: Path, override val outputRoot: Path) extends Partitioner {

  /**
   * Given a key, return the output [[Path]] to the date and hour partition for this key.
   *
   * The full path will look like `/$outputRoot/source=$key/date=$date/hour=$hour`
   *
   * @param key key to identify data in the staging directory
   * @return the output [[Path]] for this key
   */
  override def outputDirectory(key: String): Path = {
    val date = new Date()
    new Path(outputRoot,
      s"source=$key" +
        s"/ds=${datePartition(date, DateAndHourPartitioner.DayFormat)}" +
        s"/hour=${datePartition(date, DateAndHourPartitioner.HourFormat)}"
    )
  }

  /**
   * Return a formatted date/hour string to be used for the partition name.
   *
   * @param date [[Date]] for this partition
   * @param format format [[String]] compatible with [[SimpleDateFormat]]
   * @return formatted date/hour [[String]]
   */
  def datePartition(date: Date, format: String): String = new SimpleDateFormat(format).format(date)
}

object DateAndHourPartitioner {
  val HourFormat = "H"
  val DayFormat = "yyyy-MM-dd"
}
