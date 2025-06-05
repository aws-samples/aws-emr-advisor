package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.utils.Formatter.printNumber

/**
 * AppMetrics collects and summarizes high-level execution metrics for a Spark application.
 *
 * It tracks:
 *   - Counts of jobs, stages, and tasks (successful and failed)
 *   - Task-level kill events
 *   - Aggregated Spark task metrics via `AggTaskMetrics`
 *
 * This class is mutable and thread-safe for concurrent metric ingestion.
 * It is used during event log parsing and later referenced by efficiency analyzers,
 * report generators, and optimizers.
 */
class AppMetrics {

  // ------------------------------------------------------------------------
  // Internal state (counts)
  // ------------------------------------------------------------------------

  /** Number of jobs that failed. */
  private var failedJobs = 0

  /** Number of stages that failed. */
  private var failedStages = 0

  /** Number of tasks that failed. */
  private var failedTasks = 0

  /** Total jobs observed. */
  private var totalJobs = 0

  /** Total stages observed. */
  private var totalStages = 0

  /** Total tasks observed. */
  private var totalTasks = 0

  /** Number of tasks that were killed (e.g., speculative or eviction). */
  private var killedTasks = 0

  /** Aggregated metrics for all tasks in the application. */
  val appAggMetrics: AggTaskMetrics = new AggTaskMetrics

  // ------------------------------------------------------------------------
  // Mutation methods (thread-safe)
  // ------------------------------------------------------------------------

  /** Increment the count of failed jobs. */
  def addFailedJob(): Unit = synchronized { failedJobs += 1 }

  /** Increment the count of failed stages. */
  def addFailedStage(): Unit = synchronized { failedStages += 1 }

  /** Increment the count of failed tasks. */
  def addFailedTask(): Unit = synchronized { failedTasks += 1 }

  /** Increment the total number of jobs. */
  def addJob(): Unit = synchronized { totalJobs += 1 }

  /** Increment the total number of stages. */
  def addStage(): Unit = synchronized { totalStages += 1 }

  /** Increment the total number of tasks. */
  def addTask(): Unit = synchronized { totalTasks += 1 }

  /** Increment the count of killed tasks. */
  def addKilledTask(): Unit = synchronized { killedTasks += 1 }

  // ------------------------------------------------------------------------
  // Accessors
  // ------------------------------------------------------------------------

  /** Gets the number of failed jobs. */
  def getFailedJobs: Int = failedJobs

  /** Gets the number of failed stages. */
  def getFailedStages: Int = failedStages

  /** Gets the number of failed tasks. */
  def getFailedTasks: Int = failedTasks

  /** Gets the total number of jobs. */
  def getTotalJobs: Int = totalJobs

  /** Gets the total number of stages. */
  def getTotalStages: Int = totalStages

  /** Gets the total number of tasks. */
  def getTotalTasks: Int = totalTasks

  /** Gets the number of killed tasks. */
  def getKilledTasks: Int = killedTasks

  // ------------------------------------------------------------------------
  // Summary output
  // ------------------------------------------------------------------------

  /**
   * Returns a human-readable summary of job, stage, and task counts
   * including their respective failure totals.
   *
   * @return Formatted string for logging or reporting.
   */
  def summary: String =
    s"""Spark launched ${printNumber(getTotalJobs)} Jobs (${printNumber(getFailedJobs)} failed),
       | ${printNumber(getTotalStages)} Stages (${printNumber(getFailedStages)} failed) and
       | ${printNumber(getTotalTasks)} Tasks (${printNumber(getFailedTasks)} failed)""".stripMargin

}
