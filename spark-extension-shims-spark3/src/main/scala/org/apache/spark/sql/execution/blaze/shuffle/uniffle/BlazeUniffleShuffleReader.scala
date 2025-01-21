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
    FieldUtils.readField(reader.getClass, "numMaps", true).asInstanceOf[Int]
  private val partitionToExpectBlocks: util.Map[Integer, Roaring64NavigableMap] = FieldUtils
    .readField(reader.getClass, "partitionToExpectBlocks", true)
    .asInstanceOf[util.Map[Integer, Roaring64NavigableMap]]
  private val partitionToShuffleServers: util.Map[Integer, util.List[ShuffleServerInfo]] =
    FieldUtils
      .readField(reader.getClass, "partitionToShuffleServers", true)
      .asInstanceOf[util.Map[Integer, util.List[ShuffleServerInfo]]]
  private val mapStartIndex: Int =
    FieldUtils.readField(reader.getClass, "mapStartIndex", true).asInstanceOf[Int]
  private val mapEndIndex: Int =
    FieldUtils.readField(reader.getClass, "mapEndIndex", true).asInstanceOf[Int]
  private val rssConf: RssConf =
    FieldUtils.readField(reader.getClass, "rssConf", true).asInstanceOf[RssConf]
  FieldUtils
    .readField(reader.getClass, "shuffleDependency", true)
    .asInstanceOf[ShuffleDependency[K, _, C]]
  private val appId: String =
    FieldUtils.readField(reader.getClass, "appId", true).asInstanceOf[String]
  private val shuffleId: Int =
    FieldUtils.readField(reader.getClass, "shuffleId", true).asInstanceOf[Int]
  private val basePath: String =
    FieldUtils.readField(reader.getClass, "basePath", true).asInstanceOf[String]
  private val partitionNum: Int =
    FieldUtils.readField(reader.getClass, "partitionNum", true).asInstanceOf[Int]
  private val taskIdBitmap: Roaring64NavigableMap = FieldUtils
    .readField(reader.getClass, "taskIdBitmap", true)
    .asInstanceOf[Roaring64NavigableMap]
  private val hadoopConf: Configuration =
    FieldUtils.readField(reader.getClass, "hadoopConf", true).asInstanceOf[Configuration]
  private val dataDistributionType: ShuffleDataDistributionType = FieldUtils
    .readField(reader.getClass, "dataDistributionType", true)
    .asInstanceOf[ShuffleDataDistributionType]
  private val readMetrics: ShuffleReadMetrics = {
    var readMetrics: ShuffleReadMetrics = null
    if (metrics != null) readMetrics = {
      val cls: Class[_] = Class.forName("org.apache.spark.shuffle.RssShuffleManager$ReadMetrics")
      cls.getDeclaredConstructor().newInstance(metrics).asInstanceOf[ShuffleReadMetrics]
    }
    else readMetrics = context.taskMetrics.shuffleReadMetrics
    readMetrics
  }

  override protected def readBlocks(): Iterator[(BlockId, InputStream)] = {
    val inputStream = new UniffleInputStream(new MultiPartitionIterator[K, C]())
    Iterator.single((null, inputStream))
  }

  class MultiPartitionIterator[K, C] extends AbstractIterator[Product2[K, C]] {
    val iterators: util.List[RssShuffleDataIterator[K, C]] =
      new util.ArrayList[RssShuffleDataIterator[K, C]]()

    if (numMaps > 0) {
      for (partition <- startPartition until endPartition) {
        if (partitionToExpectBlocks.get(partition).isEmpty) {
          logInfo(s"$partition partition is empty partition")
        } else {}
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
          new RssShuffleDataIterWrapper[K, C](shuffleReadClient, readMetrics, rssConf)
        iterators.add(iterator)
      }
      iterator = iterators.iterator()
      if (iterator.hasNext) {
        dataIterator = iterator.next
        iterator.remove()
      }
      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          context.taskMetrics.mergeShuffleReadMetrics()
          if (dataIterator != null) {
            dataIterator.cleanup()
          }
          while (iterator.hasNext) {
            iterator.next().cleanup()
          }
        }
      })
    }

    var iterator: util.Iterator[RssShuffleDataIterator[K, C]] = null
    var dataIterator: RssShuffleDataIterator[K, C] = null

    override def hasNext: Boolean = try {
      if (dataIterator == null) return false
      while (!dataIterator.hasNext) {
        if (!iterator.hasNext) return false
        dataIterator.cleanup()
        dataIterator = iterator.next
        iterator.remove()
      }
      dataIterator.hasNext
    } catch {
      case e: RssException =>
        throw e
    }

    override def next: Product2[K, C] = {
      val result: Product2[K, C] = dataIterator.next
      result
    }
  }

  class UniffleInputStream(iterator: MultiPartitionIterator[_, _]) extends java.io.InputStream {
    private var currentByteBuffer: ByteBuffer = null

    override def read(): Int = {
      throw new UnsupportedOperationException("")
    }

    override protected def read(b: Array[Byte]): Int = {
      if (currentByteBuffer == null) {
        if (!iterator.hasNext) {
          return 0
        }
        currentByteBuffer = iterator.next()._2.asInstanceOf[ByteBuffer]
        if (currentByteBuffer == null) {
          throw new RuntimeException(
            "Gotten the empty byte buffer when retrieving from uniffle client")
        }
      }
      if (currentByteBuffer.remaining() < b.length) {
        throw new IllegalArgumentException(
          s"ByteBuffer dont has enough data into the array buffer. actual: ${currentByteBuffer
            .remaining()}, required: ${b.length}")
      }
      currentByteBuffer.get(b)
      b.length
    }
  }
}

class RssShuffleDataIterWrapper[K, V](
    readClient: ShuffleReadClient,
    shuffleReadMetrics: ShuffleReadMetrics,
    rssConf: RssConf)
    extends RssShuffleDataIterator[K, V](null, readClient, shuffleReadMetrics, rssConf) {

  override def createKVIterator(data: ByteBuffer): Iterator[Tuple2[AnyRef, AnyRef]] = {
    val element = Tuple2.apply(1.asInstanceOf[AnyRef], data.asInstanceOf[AnyRef])
    scala.Iterator.single(element)
  }
}
