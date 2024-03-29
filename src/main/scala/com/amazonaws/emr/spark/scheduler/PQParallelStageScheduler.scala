package com.amazonaws.emr.spark.scheduler

import scala.collection.mutable

/**
 * PQParallelStageScheduler is the task scheduler, which ensures that
 * we are not running more tasks than the total number of cores in the
 * system.
 *
 * TaskScheduler implementation using PriorityQueue (Min).
 *
 * @param totalCores
 * @param taskCountMap
 */
class PQParallelStageScheduler(totalCores: Int, taskCountMap: mutable.HashMap[Int, Int]) extends TaskScheduler {

  case class QueuedTask(duration: Long, finishingTime: Long, stageID: Int) extends Ordered[QueuedTask] {
    def compare(that: QueuedTask): Int = that.finishingTime.compareTo(finishingTime)
  }

  private val NO_STAGE_ID = -1
  private val taskQueue = mutable.PriorityQueue.newBuilder[QueuedTask].result()
  private var wallClock: Long = 0

  if (totalCores <= 0) throw new RuntimeException(s"Absurd number of cores ${totalCores}")

  /**
   * deques one task from the taskQueue and updates the wallclock time to
   * account for completion of this task.
   *
   * @return if the task is the last task of any stage, it returns the stageID
   *         o.w. returns NO_STAGE_ID (-1)
   */
  private def dequeOne(): Int = {
    var finishedStageID = NO_STAGE_ID
    val finishedTask = taskQueue.dequeue()
    wallClock = finishedTask.finishingTime
    var pendingTasks = taskCountMap(finishedTask.stageID)
    pendingTasks -= 1
    taskCountMap(finishedTask.stageID) = pendingTasks
    //all tasks of checkingStage finished
    if (pendingTasks == 0) {
      onStageFinished(finishedTask.stageID)
      finishedStageID = finishedTask.stageID
    }
    finishedStageID
  }

  /**
   * Schedules this task for execution on a free core. If no free core is
   * found we advance the wallclock time to make space for the new task.
   *
   * @param taskTime time that this task should take to complete
   * @param stageID  stage which this task belongs to
   */
  override def schedule(taskTime: Long, stageID: Int): Unit = {

    if (taskQueue.size == totalCores) dequeOne()

    taskQueue.enqueue(QueuedTask(taskTime, wallClock + taskTime, stageID))
  }

  /**
   * If we run out of tasks, and new tasks will only get submitted if new
   * stages are submitted. We want to finish tasks till the point some
   * stage finishes. This method pushes the simulation to the next interesting
   * point, which is completion of some stage.
   */
  override def runTillStageCompletion(): Int = {
    var finishedStageID = NO_STAGE_ID
    while (taskQueue.nonEmpty && finishedStageID == NO_STAGE_ID) {
      finishedStageID = dequeOne()
    }
    if (finishedStageID == NO_STAGE_ID) {
      throw new RuntimeException("Unable to finish any stage after handling all scheduled tasks")
    }
    finishedStageID
  }

  override def isStageComplete(stageID: Int): Boolean = {
    val tasksPending = taskCountMap.getOrElse(stageID, 0)
    tasksPending == 0
  }

  override def wallClockTime(): Long = {
    if (taskQueue.nonEmpty) {
      taskQueue.map(x => x.finishingTime).max
    } else {
      wallClock
    }
  }

}