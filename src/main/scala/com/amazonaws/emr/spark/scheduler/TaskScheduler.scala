package com.amazonaws.emr.spark.scheduler

trait TaskScheduler {
  def schedule(taskTime: Long, stageID: Int = -1): Unit
  def wallClockTime(): Long
  def runTillStageCompletion():Int
  def isStageComplete(stageID: Int): Boolean
  def onStageFinished(stageID: Int): Unit = ???
}
