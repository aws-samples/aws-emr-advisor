package com.amazonaws.emr.spark.models.timespan

import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable

/**
 * The timeSpan of a Job can seen with respect to other jobs as well
 * as driver timeSpans providing a timeLine. The other way to look at
 * Job timeline is to dig deep and check how the individual stages are
 * doing
 *
 * @param jobID Spark Job ID
 */
class JobTimeSpan(val jobID: Long) extends TimeSpan {

  var jobMetrics = new AggTaskMetrics()
  var stageMap = new mutable.HashMap[Int, StageTimeSpan]()

  def addStage(stage: StageTimeSpan): Unit = {
    stageMap(stage.stageID) = stage
  }

  def updateAggregateTaskMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    jobMetrics.update(taskMetrics, taskInfo)
  }

  def updateAggregateTaskMetrics(metrics: AggTaskMetrics): Unit = {
    jobMetrics.update(metrics)
  }

  /**
   * This function computes the minimum time it would take to run this job.
   * The computation takes into account the parallel stages.
   */
  def computeCriticalTimeForJob(): Long = {
    if (stageMap.isEmpty) {
      0L
    } else {
      val maxStageID = stageMap.keys.max
      val data = stageMap.map(x =>
        (x._1,
          (
            x._2.parentStageIDs,
            x._2.stageMetrics.getMetricMax(AggTaskMetrics.executorRunTime)
          )
        )
      )
      criticalTime(maxStageID, data)
    }
  }

  /**
   * recursive function to compute critical time starting from the last stage
   *
   * @param stageID
   * @param data
   * @return
   */
  private def criticalTime(stageID: Int, data: mutable.HashMap[Int, (Seq[Int], Long)]): Long = {
    val stageData = data.getOrElse(stageID, (List.empty[Int], 0L))
    stageData._2 + {
      if (stageData._1.isEmpty) 0L
      else stageData._1.map(x => criticalTime(x, data)).max
    }
  }

}