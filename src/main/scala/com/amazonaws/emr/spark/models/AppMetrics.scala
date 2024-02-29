package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.utils.Formatter.printNumber

class AppMetrics {

  private var failedJobs = 0
  private var failedStages = 0
  private var failedTasks = 0

  private var totalJobs = 0
  private var totalStages = 0
  private var totalTasks = 0

  private var killedTasks = 0

  val appAggMetrics = new AggTaskMetrics

  def addFailedJob(): Unit = synchronized {
    failedJobs += 1
  }

  def addFailedStage(): Unit = synchronized {
    failedStages += 1
  }

  def addFailedTask(): Unit = synchronized {
    failedTasks += 1
  }

  def addJob(): Unit = synchronized {
    totalJobs += 1
  }

  def addStage(): Unit = synchronized {
    totalStages += 1
  }

  def addTask(): Unit = synchronized {
    totalTasks += 1
  }

  def addKilledTask(): Unit = synchronized {
    killedTasks += 1
  }

  def getFailedJobs: Int = failedJobs

  def getFailedStages: Int = failedStages

  def getFailedTasks: Int = failedTasks

  def getTotalJobs: Int = totalJobs

  def getTotalStages: Int = totalStages

  def getTotalTasks: Int = totalTasks

  def getKilledTasks: Int = killedTasks

  def summary: String =
    s"""Spark launched ${printNumber(getTotalJobs)} Jobs (${printNumber(getFailedJobs)} failed),
       | ${printNumber(getTotalStages)} Stages (${printNumber(getFailedStages)} failed) and
       | ${printNumber(getTotalTasks)} Tasks (${printNumber(getFailedTasks)} failed)""".stripMargin

}
