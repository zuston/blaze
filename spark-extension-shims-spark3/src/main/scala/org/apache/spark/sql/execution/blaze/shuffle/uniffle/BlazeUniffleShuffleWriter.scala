package org.apache.spark.sql.execution.blaze.shuffle.uniffle

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.shuffle.{RssShuffleHandle, RssShuffleManager, ShuffleHandleInfo, ShuffleWriteMetricsReporter}
import org.apache.spark.shuffle.writer.RssShuffleWriter
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleWriterBase
import org.apache.uniffle.client.api.ShuffleWriteClient

class BlazeUniffleShuffleWriter[K, V, C](appId: String,
                                         shuffleId: Int,
                                         taskId: String,
                                         taskAttemptId: Long,
                                         metrics: ShuffleWriteMetrics,
                                         shuffleManager: RssShuffleManager,
                                         sparkConf: SparkConf,
                                         shuffleWriteClient: ShuffleWriteClient,
                                         handle: RssShuffleHandle[K, V, C],
                                         taskFailureCallback: java.util.function.Function[String, Boolean],
                                         shuffleHandleInfo: ShuffleHandleInfo,
                                         context: TaskContext)
  extends RssShuffleWriter[K, V, C](appId, shuffleId, taskId, taskAttemptId, metrics, shuffleManager, sparkConf, shuffleWriteClient, handle, taskFailureCallback, context)
  with BlazeRssShuffleWriterBase[K, C](metrics) {

}
