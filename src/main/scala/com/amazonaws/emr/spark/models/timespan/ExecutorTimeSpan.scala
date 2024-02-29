package com.amazonaws.emr.spark.models.timespan

import com.amazonaws.emr.spark.models.metrics.{AggExecutorMetrics, AggTaskMetrics}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.scheduler.cluster.ExecutorInfo

import scala.util.Try

class ExecutorTimeSpan(val executorID: String) extends TimeSpan {

  var hostID: String = ""

  var cores: Int = 0

  var executorInfo = new ExecutorInfo(hostID, 1, Map())
  var executorMetrics = new AggExecutorMetrics
  var executorTaskMetrics = new AggTaskMetrics

  var memoryMax = 0L
  var memoryMaxOnHeap = 0L
  var memoryMaxOffHeap = 0L

  def updateMetrics(em: ExecutorMetrics): Unit = executorMetrics
    .update(em)

  def updateMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = executorTaskMetrics
    .update(taskMetrics, taskInfo)

  def getCoreMemoryRatio: Long = Try {
    executorMetrics.getMetricMax(AggExecutorMetrics.JVMHeapMemory) / cores
  }.getOrElse(0L)

}
