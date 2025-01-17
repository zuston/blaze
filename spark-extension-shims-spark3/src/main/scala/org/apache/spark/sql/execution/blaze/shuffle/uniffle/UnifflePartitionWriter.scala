/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.blaze.shuffle.uniffle

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.writer.WriteBufferManager
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase

import java.nio.ByteBuffer

class UnifflePartitionWriter(numPartitions: Int,
                             metrics: ShuffleWriteMetricsReporter,
                             bufferManager: WriteBufferManager) extends RssPartitionWriterBase with Logging {
  private val mapStatusLengths: Array[Long] = Array.fill(numPartitions)(0L)

  override def write(partitionId: Int, buffer: ByteBuffer): Unit = {
    val numBytes = buffer.limit()
    val bytes = new Array[Byte](numBytes)
    buffer.get(bytes)
    val bytesWritten = bytes.length
    bufferManager.addPartitionData(partitionId, bytes)
    metrics.incBytesWritten(bytesWritten)
    mapStatusLengths(partitionId) += bytesWritten
  }

  override def flush(): Unit = {}

  override def close(): Unit = {}

  override def getPartitionLengthMap: Array[Long] = mapStatusLengths

  override def stop(): Unit = {}
}
