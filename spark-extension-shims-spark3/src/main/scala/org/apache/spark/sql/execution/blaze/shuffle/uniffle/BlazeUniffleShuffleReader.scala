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

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.reader.{RssShuffleDataIterator, RssShuffleReader}
import org.apache.spark.shuffle.uniffle.RssShuffleHandleWrapper
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleReaderBase
import org.apache.spark.storage.BlockId
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{ShuffleDependency, TaskContext}
import org.apache.uniffle.client.api.ShuffleReadClient
import org.apache.uniffle.client.factory.ShuffleClientFactory
import org.apache.uniffle.client.util.RssClientConfig
import org.apache.uniffle.common.config.RssConf
import org.apache.uniffle.common.exception.RssException
import org.apache.uniffle.common.{ShuffleDataDistributionType, ShuffleServerInfo}
import org.apache.uniffle.shaded.org.roaringbitmap.longlong.Roaring64NavigableMap

import java.io.InputStream
import java.nio.ByteBuffer
import java.util
import scala.collection.AbstractIterator

class BlazeUniffleShuffleReader[K, C](
    reader: RssShuffleReader[K, C],
    handle: RssShuffleHandleWrapper[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter)
    extends BlazeRssShuffleReaderBase[K, C](handle, context)
    with Logging {
  private val numMaps: Int =
    FieldUtils.readField(reader, "numMaps", true).asInstanceOf[Int]
  private val partitionToExpectBlocks: util.Map[Integer, Roaring64NavigableMap] = FieldUtils
    .readField(reader, "partitionToExpectBlocks", true)
    .asInstanceOf[util.Map[Integer, Roaring64NavigableMap]]
  private val partitionToShuffleServers: util.Map[Integer, util.List[ShuffleServerInfo]] =
    FieldUtils
      .readField(reader, "partitionToShuffleServers", true)
      .asInstanceOf[util.Map[Integer, util.List[ShuffleServerInfo]]]
  private val mapStartIndex: Int =
    FieldUtils.readField(reader, "mapStartIndex", true).asInstanceOf[Int]
  private val mapEndIndex: Int =
    FieldUtils.readField(reader, "mapEndIndex", true).asInstanceOf[Int]
  private val rssConf: RssConf =
    FieldUtils.readField(reader, "rssConf", true).asInstanceOf[RssConf]
  private val dependency = FieldUtils
    .readField(reader, "shuffleDependency", true)
    .asInstanceOf[ShuffleDependency[K, _, C]]
  private val appId: String =
    FieldUtils.readField(reader, "appId", true).asInstanceOf[String]
  private val shuffleId: Int =
    FieldUtils.readField(reader, "shuffleId", true).asInstanceOf[Int]
  private val basePath: String =
    FieldUtils.readField(reader, "basePath", true).asInstanceOf[String]
  private val partitionNum: Int =
    FieldUtils.readField(reader, "partitionNum", true).asInstanceOf[Int]
  private val taskIdBitmap: Roaring64NavigableMap = FieldUtils
    .readField(reader, "taskIdBitmap", true)
    .asInstanceOf[Roaring64NavigableMap]
  private val hadoopConf: Configuration =
    FieldUtils.readField(reader, "hadoopConf", true).asInstanceOf[Configuration]
  private val dataDistributionType: ShuffleDataDistributionType = FieldUtils
    .readField(reader, "dataDistributionType", true)
    .asInstanceOf[ShuffleDataDistributionType]
  private val readMetrics: ShuffleReadMetrics =
    FieldUtils.readField(reader, "readMetrics", true).asInstanceOf[ShuffleReadMetrics]

  override protected def readBlocks(): Iterator[(BlockId, InputStream)] = {
    val inputStream =
      new UniffleInputStream(new MultiPartitionIterator[K, C](), startPartition, endPartition)
    Iterator.single((null, inputStream))
  }

  class MultiPartitionIterator[K, C] extends AbstractIterator[Product2[K, C]] {
    var iterators: util.Iterator[RssShuffleDataIterator[K, C]] = null
    var currentIterator: RssShuffleDataIterator[K, C] = null

    if (numMaps > 0) {
      val shuffleDataIterList: util.List[RssShuffleDataIterator[K, C]] =
        new util.ArrayList[RssShuffleDataIterator[K, C]]()

      for (partition <- startPartition until endPartition) {
        if (partitionToExpectBlocks.get(partition).isEmpty) {
          logInfo(s"partition=$partition's shuffle data is empty")
        } else {
          val shuffleServerInfoList: util.List[ShuffleServerInfo] =
            partitionToShuffleServers.get(partition)
          // This mechanism of expectedTaskIdsBitmap filter is to filter out the most of data.
          // especially for AQE skew optimization
          val expectedTaskIdsBitmapFilterEnable: Boolean =
            !(mapStartIndex == 0 && mapEndIndex == Integer.MAX_VALUE) || shuffleServerInfoList.size > 1
          val retryMax: Int = rssConf.getInteger(
            RssClientConfig.RSS_CLIENT_RETRY_MAX,
            RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE)
          val retryIntervalMax: Long = rssConf.getLong(
            RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
            RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE)
          val shuffleReadClient: ShuffleReadClient =
            ShuffleClientFactory.getInstance.createShuffleReadClient(
              ShuffleClientFactory.newReadBuilder
                .appId(appId)
                .shuffleId(shuffleId)
                .partitionId(partition)
                .basePath(basePath)
                .partitionNumPerRange(1)
                .partitionNum(partitionNum)
                .blockIdBitmap(partitionToExpectBlocks.get(partition))
                .taskIdBitmap(taskIdBitmap)
                .shuffleServerInfoList(shuffleServerInfoList)
                .hadoopConf(hadoopConf)
                .shuffleDataDistributionType(dataDistributionType)
                .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
                .retryMax(retryMax)
                .retryIntervalMax(retryIntervalMax)
                .rssConf(rssConf))
          val iterator: RssShuffleDataIterWrapper[K, C] =
            new RssShuffleDataIterWrapper[K, C](
              dependency,
              shuffleReadClient,
              readMetrics,
              rssConf)
          shuffleDataIterList.add(iterator)
        }
      }
      iterators = shuffleDataIterList.iterator()
      if (iterators.hasNext) {
        currentIterator = iterators.next
        iterators.remove()
      }
      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          context.taskMetrics.mergeShuffleReadMetrics()
          if (currentIterator != null) {
            currentIterator.cleanup()
          }
          if (iterators != null) {
            while (iterators.hasNext) {
              iterators.next().cleanup()
            }
          }
        }
      })
    }

    override def hasNext: Boolean = try {
      if (currentIterator == null) return false
      while (!currentIterator.hasNext) {
        if (!iterators.hasNext) return false
        currentIterator.cleanup()
        currentIterator = iterators.next
        iterators.remove()
      }
      currentIterator.hasNext
    } catch {
      case e: RssException =>
        throw e
    }

    override def next: Product2[K, C] = {
      val result: Product2[K, C] = currentIterator.next
      result
    }
  }

  class UniffleInputStream(
      iterator: MultiPartitionIterator[_, _],
      startPartition: Int,
      endPartition: Int)
      extends java.io.InputStream {
    private var position: Int = 0
    private var limit: Int = 0
    private var byteArr: Array[Byte] = null

    override def read(): Int = {
      if (byteArr == null) {
        if (!iterator.hasNext) {
          return -1
        }
        val next = iterator.next()
        if (next == null) {
          return -1
        }
        byteArr = next._2.asInstanceOf[ByteBuffer].array()
        if (byteArr == null) {
          return -1
        }
        position = 0
        limit = byteArr.length
      }
      if (position < limit) {
        val result = byteArr(position).toInt
        position += 1
        return result
      } else {
        byteArr = null
        return read()
      }
    }
  }
}

class RssShuffleDataIterWrapper[K, V](
    dependency: ShuffleDependency[_, _, _],
    readClient: ShuffleReadClient,
    shuffleReadMetrics: ShuffleReadMetrics,
    rssConf: RssConf)
    extends RssShuffleDataIterator[K, V](
      dependency.serializer,
      readClient,
      shuffleReadMetrics,
      rssConf) {

  override def createKVIterator(data: ByteBuffer): Iterator[Tuple2[AnyRef, AnyRef]] = {
    val element = Tuple2.apply(1.asInstanceOf[AnyRef], data.asInstanceOf[AnyRef])
    scala.Iterator.single(element)
  }
}
