package com.amazonaws.emr.spark.models.metrics

import com.amazonaws.emr.utils.Formatter.{formatMilliSeconds, formatNanoSeconds, humanReadableBytes}

import scala.collection.mutable

class AggregateMetrics {

  @transient protected val formatMetric = new mutable.HashMap[AggTaskMetrics.Metric, ((AggTaskMetrics
  .Metric, AggregateValue)) => List[String]]()
  formatMetric(AggTaskMetrics.shuffleWriteTime) = formatNanoTime
  formatMetric(AggTaskMetrics.shuffleWriteBytesWritten) = formatBytes
  formatMetric(AggTaskMetrics.shuffleWriteRecordsWritten) = formatRecords
  formatMetric(AggTaskMetrics.shuffleReadFetchWaitTime) = formatNanoTime
  formatMetric(AggTaskMetrics.shuffleReadBytesRead) = formatBytes
  formatMetric(AggTaskMetrics.shuffleReadRecordsRead) = formatRecords
  formatMetric(AggTaskMetrics.shuffleReadLocalBlocks) = formatRecords
  formatMetric(AggTaskMetrics.shuffleReadRemoteBlocks) = formatRecords

  formatMetric(AggTaskMetrics.inputBytesRead) = formatBytes
  formatMetric(AggTaskMetrics.inputRecordsRead) = formatRecords
  formatMetric(AggTaskMetrics.outputBytesWritten) = formatBytes
  formatMetric(AggTaskMetrics.outputRecordsWritten) = formatRecords

  formatMetric(AggTaskMetrics.memoryBytesSpilled) = formatBytes
  formatMetric(AggTaskMetrics.diskBytesSpilled) = formatBytes

  formatMetric(AggTaskMetrics.executorDeserializeCpuTime) = formatNanoTime
  formatMetric(AggTaskMetrics.executorDeserializeTime) = formatMillisTime
  formatMetric(AggTaskMetrics.executorRunTime) = formatMillisTime
  formatMetric(AggTaskMetrics.executorCpuTime) = formatNanoTime

  formatMetric(AggTaskMetrics.jvmGCTime) = formatMillisTime
  formatMetric(AggTaskMetrics.resultSize) = formatBytes
  formatMetric(AggTaskMetrics.resultSerializationTime) = formatMillisTime

  formatMetric(AggTaskMetrics.peakExecutionMemory) = formatBytes
  formatMetric(AggTaskMetrics.taskDuration) = formatMillisTime

  @transient private val numberFormatter = java.text.NumberFormat.getIntegerInstance

  private def formatNanoTime(x: (AggTaskMetrics.Metric, AggregateValue)): List[String] = List(
    AggTaskMetrics.metricName(x._1.toString),
    formatNanoSeconds(x._2.value),
    formatNanoSeconds(x._2.min),
    formatNanoSeconds(x._2.mean.toLong),
    formatNanoSeconds(x._2.max)
  )

  private def formatMillisTime(x: (AggTaskMetrics.Metric, AggregateValue)): List[String] = List(
    AggTaskMetrics.metricName(x._1.toString),
    formatMilliSeconds(x._2.value),
    formatMilliSeconds(x._2.min),
    formatMilliSeconds(x._2.mean.toLong),
    formatMilliSeconds(x._2.max)
  )

  private def formatBytes(x: (AggTaskMetrics.Metric, AggregateValue)): List[String] = List(
    AggTaskMetrics.metricName(x._1.toString),
    humanReadableBytes(x._2.value),
    humanReadableBytes(x._2.min),
    humanReadableBytes(x._2.mean.toLong),
    humanReadableBytes(x._2.max)
  )

  private def formatRecords(x: (AggTaskMetrics.Metric, AggregateValue)): List[String] = List(
    AggTaskMetrics.metricName(x._1.toString),
    numberFormatter.format(x._2.value),
    numberFormatter.format(x._2.min),
    numberFormatter.format(x._2.mean.toLong),
    numberFormatter.format(x._2.max)
  )

}
