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

import org.apache.spark.TaskContext
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.reader.RssShuffleDataIterator
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleReaderBase
import org.apache.spark.storage.BlockId
import org.apache.uniffle.client.api.ShuffleReadClient
import org.apache.uniffle.common.config.RssConf

import java.io.InputStream
import java.nio.ByteBuffer

class BlazeUniffleShuffleReader[K, C](
    handle: CelebornShuffleHandle[K, _, C],
    context: TaskContext)
    extends BlazeRssShuffleReaderBase[K, C](handle, context)
    with Logging {

  override protected def readBlocks(): Iterator[(BlockId, InputStream)] = ???
}

class UniffleInputStream(iterator: RssShuffleDataIterWrapper[_, _]) extends java.io.InputStream {

  override def read(): Int = ???

  override protected def read(b: Array[Byte]): Int = ???
}

class RssShuffleDataIterWrapper[K, V](
    readClient: ShuffleReadClient,
    shuffleReadMetrics: ShuffleReadMetrics,
    rssConf: RssConf)
    extends RssShuffleDataIterator[K, V](null, readClient, shuffleReadMetrics, rssConf) {

  override def createKVIterator(data: ByteBuffer): Iterator[Tuple2[AnyRef, AnyRef]] = ???
}
