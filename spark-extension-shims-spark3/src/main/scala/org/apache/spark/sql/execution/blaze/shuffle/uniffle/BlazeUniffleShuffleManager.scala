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

import org.apache.spark.shuffle.writer.RssShuffleWriter
import org.apache.spark.shuffle.{RssShuffleManager, ShuffleBlockResolver, ShuffleHandle, ShuffleReadMetricsReporter, ShuffleReader, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.sql.execution.blaze.shuffle.{BlazeRssShuffleManagerBase, BlazeRssShuffleReaderBase, BlazeRssShuffleWriterBase}

class BlazeUniffleShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends BlazeRssShuffleManagerBase(conf) {
  private val uniffleShuffleManager: RssShuffleManager = new RssShuffleManager(conf, isDriver);
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    uniffleShuffleManager.registerShuffle(shuffleId, dependency)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    uniffleShuffleManager.unregisterShuffle(shuffleId)
  }

  override def getBlazeRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): BlazeRssShuffleReaderBase[K, C] = ???

  override def getBlazeRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): BlazeRssShuffleReaderBase[K, C] = ???

  override def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    uniffleShuffleManager.getReader(handle, startPartition, endPartition, context, metrics)
  }

  override def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    uniffleShuffleManager.getReaderForRange(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def getBlazeRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): BlazeRssShuffleWriterBase[K, V] = {
    val writer: ShuffleWriter[K, V] =
      uniffleShuffleManager.getWriter(handle, mapId, context, metrics)
    new BlazeUniffleShuffleWriter(writer.asInstanceOf[RssShuffleWriter[K, V, _]], metrics)
  }

  override def getRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    uniffleShuffleManager.getWriter(handle, mapId, context, metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    uniffleShuffleManager.shuffleBlockResolver()
  }

  override def stop(): Unit = {
    uniffleShuffleManager.stop()
  }
}
