package com.amazonaws.emr.spark.models.timespan

import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

class HostTimeSpan(val hostID: String) extends TimeSpan {

  private val hostMetrics = new AggTaskMetrics

  override def duration: Option[Long] = Some(super.duration.getOrElse(System.currentTimeMillis() - startTime))

  def updateMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = hostMetrics.update(taskMetrics, taskInfo)

}
