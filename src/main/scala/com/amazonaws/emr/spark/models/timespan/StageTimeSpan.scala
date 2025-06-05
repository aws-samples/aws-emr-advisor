package com.amazonaws.emr.spark.models.timespan

import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.utils.Formatter.printDuration
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable

/**
 * Job Stage metrics
 *
 * @param stageID Stage index ID
 */
class StageTimeSpan(val stageID: Int) extends TimeSpan {

  var numberOfTasks: Long = 0L
  var stageMetrics = new AggTaskMetrics()
  var parentStageIDs: Seq[Int] = null

  // we keep execution time of each task
  var taskExecutionTimes: Array[Long] = Array.emptyLongArray
  var taskPeakMemoryUsage: Array[Long] = Array.emptyLongArray

  private val tempTaskTimes = new mutable.ListBuffer[(Long, Long, Long)]
  private var minTaskLaunchTime: Long = Long.MaxValue
  private var maxTaskFinishTime = 0L

  def updateAggregateTaskMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    stageMetrics.update(taskMetrics, taskInfo)
  }

  def setParentStageIDs(parentIDs: Seq[Int]): Unit = {
    parentStageIDs = parentIDs
  }

  def updateTasks(taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit = synchronized {
    if (taskInfo != null && taskMetrics != null) {
      val totalRunTime = taskMetrics.executorRunTime + taskMetrics.executorDeserializeTime + taskMetrics.resultSerializationTime
      tempTaskTimes += ((
        taskInfo.taskId,
        totalRunTime,
        taskMetrics.peakExecutionMemory
        ))
      if (taskInfo.launchTime < minTaskLaunchTime) minTaskLaunchTime = taskInfo.launchTime
      if (taskInfo.finishTime > maxTaskFinishTime) maxTaskFinishTime = taskInfo.finishTime
    }
  }

  def finalUpdate(): Unit = synchronized {

    setStartTime(minTaskLaunchTime)
    setEndTime(maxTaskFinishTime)

    taskExecutionTimes = new Array[Long](tempTaskTimes.size)

    var currentIndex = 0
    tempTaskTimes
      .sortWith((left, right) => left._1 < right._1)
      .foreach { x =>
        taskExecutionTimes(currentIndex) = x._2
        currentIndex += 1
      }

    val countPeakMemoryUsage = {
      if (tempTaskTimes.size > 64) {
        64
      } else {
        tempTaskTimes.size
      }
    }

    taskPeakMemoryUsage = tempTaskTimes
      .map(x => x._3)
      .sortWith((a, b) => a > b)
      .take(countPeakMemoryUsage).toArray

    // We don't want to keep all this objects hanging around for long time
    tempTaskTimes.clear()
  }

}

case class StageSummaryMetrics(
  id: Int,
  numberOfTasks: Long,
  stageAvgPeakMemory: Long,
  stageAvgMemorySpilled: Long,
  stageMaxPeakMemory: Long,
  stageTotalMemorySpilled: Long
)