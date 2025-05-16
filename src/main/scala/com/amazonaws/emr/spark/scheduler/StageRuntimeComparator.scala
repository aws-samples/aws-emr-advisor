package com.amazonaws.emr.spark.scheduler


import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.timespan.StageTimeSpan
import com.amazonaws.emr.utils.Formatter.printDuration

import scala.collection.mutable.ArrayBuffer

object StageRuntimeComparator {

  case class StageDebugResult(
    stageId: Int,
    actualDuration: Long,
    estimatedDuration: Long,
    taskCount: Int,
    avgTaskTime: Long,
    maxTaskTime: Long,
    parentStages: Seq[Int]
  )

  def compareRealVsEstimatedStageTimes(appContext: AppContext): Seq[StageDebugResult] = {

    val totalCores: Int = math.max(appContext.appSparkExecutors.executorsTotalCores.toInt, 1)

    appContext.stageMap.toSeq.map { case (stageId, stageSpan: StageTimeSpan) =>
      val taskTimes: Array[Long] = stageSpan.taskExecutionTimes
      val taskCount: Int = taskTimes.length

      val estimatedDuration: Long = if (taskCount == 0) {
        0L
      } else {
        val sortedTasks: Array[Long] = taskTimes.sorted(Ordering.Long.reverse)
        val slots: Array[Long] = new Array[Long](totalCores)

        for (t <- sortedTasks) {
          var minIdx = 0
          var minValue = slots(0)
          var i = 1
          while (i < slots.length) {
            if (slots(i) < minValue) {
              minIdx = i
              minValue = slots(i)
            }
            i += 1
          }
          slots(minIdx) += t
        }
        slots.max
      }

      val actualDuration: Long = stageSpan.endTime - stageSpan.startTime
      val avgTime: Long = if (taskCount > 0) taskTimes.sum / taskCount else 0L
      val maxTime: Long = if (taskCount > 0) taskTimes.max else 0L

      StageDebugResult(
        stageId = stageId,
        actualDuration = actualDuration,
        estimatedDuration = estimatedDuration,
        taskCount = taskCount,
        avgTaskTime = avgTime,
        maxTaskTime = maxTime,
        parentStages = stageSpan.parentStageIDs
      )
    }.sortBy(_.stageId)
  }

  def printDebug(results: Seq[StageDebugResult]): Unit = {
    val header = f"${"StageID"}%-8s ${"Actual(ms)"}%-12s ${"Estimated(ms)"}%-14s ${"Tasks"}%-8s ${"AvgTask(ms)"}%-12s ${"MaxTask(ms)"}%-12s ${"Parents"}"
    println(header)
    println("-" * header.length)

    for (r <- results) {
      val line = f"${r.stageId}%-8d ${r.actualDuration}%-12d ${r.estimatedDuration}%-14d ${r.taskCount}%-8d ${r.avgTaskTime}%-12d ${r.maxTaskTime}%-12d ${r.parentStages.mkString(",")}"
      println(line)
    }

    val totalRealStageMs = results.map(_.actualDuration).sum
    val totalEstimatedStageMs = results.map(_.estimatedDuration).sum
    println(s"Real Stage time      : ${printDuration(totalRealStageMs)}")
    println(s"Estimated Stage time : ${printDuration(totalEstimatedStageMs)}")

  }
}