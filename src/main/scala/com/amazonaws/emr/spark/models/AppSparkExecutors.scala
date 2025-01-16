package com.amazonaws.emr.spark.models

import com.amazonaws.emr.Config
import com.amazonaws.emr.spark.models.metrics.{AggExecutorMetrics, AggTaskMetrics}
import com.amazonaws.emr.spark.models.timespan.ExecutorTimeSpan
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes}

import scala.util.Try

class AppSparkExecutors(val executors: Map[String, ExecutorTimeSpan], val appConfigs: AppConfigs) {

  val defaultDriverCores: Int = appConfigs.driverCores
  val defaultDriverMemory: Long = appConfigs.driverMemory
  val defaultExecutorCores: Int = appConfigs.executorCores
  val defaultExecutorMemory: Long = appConfigs.executorMemory

  val executorsLaunched: Int = executors.size - 1
  val executorsMaxRunning: Int = getMaxConcurrent
  val executorsTotalCores: Long = executorsMaxRunning * defaultExecutorCores
  val executorsTotalMemory: Long = executorsMaxRunning * defaultExecutorMemory

  @deprecated
  def getRequiredStoragePerExecutor: Long = {
    val maxShuffleWrite = executors.values.map(x =>
      Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.shuffleWriteBytesWritten)
      ).getOrElse(0L)).max
    val maxDiskSpilledWrite = executors.values.map(x =>
      Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.diskBytesSpilled)
      ).getOrElse(0L)).max
    maxShuffleWrite + maxDiskSpilledWrite
  }

  /**
   * Verify if the shuffle writes across all executors are uniform or if we have unbalanced writes.
   * We consider writes uniform if for all executors, shuffle data are not greater than avg writes
   * (total shuffle / concurrent executors + 0.2 * total shuffle) or lower than
   * (total shuffle / concurrent executors - 0.2 * total shuffle)
   */
  def isShuffleWriteUniform: Boolean = {
    val totalBytes = getTotalShuffleBytesWritten
    val toleration = totalBytes * 0.2
    val avgShuffleBytes = totalBytes / getMaxConcurrent
    getOnlyExecutors.forall { e =>
      val writes = e._2.executorTaskMetrics.getMetricSum(AggTaskMetrics.shuffleWriteBytesWritten)
      (avgShuffleBytes - toleration).toLong <= writes && writes <= (avgShuffleBytes + toleration.toLong)
    }
  }

  /** Total number of tasks processed overall */
  def getTotalTasksProcessed: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.count).getOrElse(0L))
    .sum

  /** Max bytes spilled to disk of a single executor */
  def getMaxDiskBytesSpilled: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.diskBytesSpilled)).getOrElse(0L))
    .max

  /** Total bytes spilled to disk across all executors (Bytes) */
  def getTotalDiskBytesSpilled: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.diskBytesSpilled)).getOrElse(0L))
    .sum

  /** Total bytes written across all executors (Bytes) */
  def getTotalShuffleBytesWritten: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.shuffleWriteBytesWritten)).getOrElse(0L))
    .sum

  /** Get total size of input data read across all executors (Bytes) */
  def getTotalInputBytesRead: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.inputBytesRead)).getOrElse(0L))
    .sum

  /** Get total size of output data witten across all executors (Bytes) */
  def getTotalOutputBytesWritten: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.outputBytesWritten)).getOrElse(0L))
    .sum

  /** Return the minimum provisioning time across executors in ms */
  def getMinLaunchTime: Long = getOnlyExecutors
    .map(x => x._2.startTime - x._2.executorInfo.requestTime.getOrElse(x._2.startTime))
    .min

  /** Return the maximum provisioning time across executors in ms */
  def getMaxLaunchTime: Long = getOnlyExecutors
    .map(x => x._2.startTime - x._2.executorInfo.requestTime.getOrElse(x._2.startTime))
    .max

  /** Return the maximum memory used for to process a task */
  def getMaxTaskMemoryUsed: Long = getOnlyExecutors.values
    .map(x => Try(x.executorTaskMetrics.getMetricMax(AggTaskMetrics.peakExecutionMemory)).getOrElse(0L))
    .max

  def getMaxJvmMemoryUsed: Long = getOnlyExecutors.values
    .map(x => Try(x.executorMetrics.getMetricMax(AggExecutorMetrics.JVMHeapMemory)).getOrElse(0L))
    .max

  def isComputeIntensive: Boolean = getMaxTaskMemoryUsed <= byteStringAsBytes(Config.ComputeIntensiveMaxMemory)

  def isMemoryIntensive: Boolean = getMaxTaskMemoryUsed >= byteStringAsBytes(Config.MemoryIntensiveMinMemory)

  def summary: String =
    s"""The application launched a total of <b>$executorsLaunched</b> Spark executors Maximum number of concurrent
       |executors was <b>$executorsMaxRunning</b>, for a total of <b>$executorsTotalCores</b> cores and
       |<b>${humanReadableBytes(executorsTotalMemory)}</b> memory""".stripMargin

  def summaryResources: String =
    s"""Maximum number of concurrent executors was <b>$executorsMaxRunning</b>, for a total of
       |<b>$executorsTotalCores</b> cores and <b>${humanReadableBytes(executorsTotalMemory)}</b> memory""".stripMargin

  def getOnlyExecutors: Map[String, ExecutorTimeSpan] = executors
    .filter(!_._1.equalsIgnoreCase("driver"))

  private def getMaxConcurrent: Int = {

    // sort all start and end times on basis of timing
    // exclude the driver from the computation
    val sorted = getOnlyExecutors.values
      .filter(t => t.startTime != 0)
      .flatMap(timeSpan => Seq[(Long, Long)]((timeSpan.startTime, 1L), (timeSpan.endTime, -1L)))
      .toArray
      .sortWith((t1: (Long, Long), t2: (Long, Long)) => {
        // for same time entry, we add them first, and then remove
        if (t1._1 == t2._1) t1._2 > t2._2 else t1._1 < t2._1
      })

    var count = 0L
    var maxConcurrent = 0L

    sorted.foreach(tuple => {
      count = count + tuple._2
      maxConcurrent = math.max(maxConcurrent, count)
    })

    // when running in local mode, we don't get
    // executor added event. Default to 1 instead of 0
    math.max(maxConcurrent, 1).toInt
  }

}
