package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.spark.models.timespan.{JobTimeSpan, StageTimeSpan}
import com.amazonaws.emr.spark.scheduler.JobOverlapHelper
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes}
import org.apache.logging.log4j.scala.Logging

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.Try

class AppEfficiencyAnalyzer extends AppAnalyzer with Logging {

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logger.info("Analyze efficiency...")

    // wall clock time, appEnd - appStart
    val appTotalTime = endTime - startTime
    // wall clock time per Job. Aggregated
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(appContext)

    // Minimum time required to run a job even when we have infinite number
    // of executors, essentially the max time taken by any task in the stage.
    // which is in the critical path. Note that some stages can run in parallel
    // we cannot reduce the job time to less than this number.
    // Aggregating over all jobs, to get the lower bound on this time.
    val criticalPathTime = JobOverlapHelper.criticalPathForAllJobs(appContext)

    /* sum of cores in all the executors:
     * There are executors coming up and going down.
     * We are taking the max-number of executors running at any point of time, and
     * multiplying it by num-cores per executor (assuming homogenous cluster)
     */
    val totalCores = appContext.appSparkExecutors.executorsTotalCores

    // total compute millis available to the application
    val appComputeMillisAvailable = totalCores * appTotalTime

    // some of the compute millis are lost when driver is doing some work
    // and has not assigned any work to the executors
    // We assume executors are only busy when one of the job is in progress
    val inJobComputeMillisAvailable = totalCores * jobTime

    // sum of millis used by all tasks of all jobs
    val inJobComputeMillisUsed = appContext.jobMap.values
      .filter(x => x.endTime > 0)
      .filter(x => x.jobMetrics.map.isDefinedAt(AggTaskMetrics.executorRunTime))
      .map(x => x.jobMetrics.map(AggTaskMetrics.executorRunTime).value)
      .sum

    val perfectJobTime = inJobComputeMillisUsed / totalCores
    val driverTimeJobBased = appTotalTime - jobTime
    val driverComputeMillisWastedJobBased = driverTimeJobBased * totalCores

    val executorUsedPercent = inJobComputeMillisUsed * 100 / inJobComputeMillisAvailable.toFloat
    val executorWastedPercent =
      (inJobComputeMillisAvailable - inJobComputeMillisUsed) * 100 / inJobComputeMillisAvailable.toFloat

    val driverWastedPercentOverAll = driverComputeMillisWastedJobBased * 100 / appComputeMillisAvailable.toFloat
    val executorWastedPercentOverAll =
      (inJobComputeMillisAvailable - inJobComputeMillisUsed) * 100 / appComputeMillisAvailable.toFloat

    appContext.appEfficiency.appTotalTime = appTotalTime
    appContext.appEfficiency.appTotalCoreAvailable = totalCores
    appContext.appEfficiency.driverTime = driverTimeJobBased
    appContext.appEfficiency.driverTimePercentage = driverTimeJobBased * 100 / appTotalTime.toFloat
    appContext.appEfficiency.executorsTime = jobTime
    appContext.appEfficiency.executorsTimePercentage = jobTime * 100 / appTotalTime.toFloat
    appContext.appEfficiency.executionTimeInfiniteResources = driverTimeJobBased + criticalPathTime
    appContext.appEfficiency.executionTimePerfectParallelism = driverTimeJobBased + perfectJobTime
    appContext.appEfficiency.executionTimeSingleExecutorOneCore = driverTimeJobBased + inJobComputeMillisUsed

    appContext.appEfficiency.appComputeMillisAvailable = appComputeMillisAvailable
    appContext.appEfficiency.driverComputeMillisWastedJobBased = driverComputeMillisWastedJobBased

    appContext.appEfficiency.inJobComputeMillisAvailable = inJobComputeMillisAvailable
    appContext.appEfficiency.inJobComputeMillisUsed = inJobComputeMillisUsed
    appContext.appEfficiency.inJobComputeMillisWasted = inJobComputeMillisAvailable - inJobComputeMillisUsed

    appContext.appEfficiency.executorUsed = inJobComputeMillisAvailable - inJobComputeMillisUsed
    appContext.appEfficiency.executorUsedPercent = executorUsedPercent
    appContext.appEfficiency.executorWastedPercent = executorWastedPercent

    appContext.appEfficiency.driverWastedPercentOverAll = driverWastedPercentOverAll
    appContext.appEfficiency.executorWastedPercentOverAll = executorWastedPercentOverAll

    // Evaluate Driver performance
    val totalResultSize = appContext.appMetrics.appAggMetrics.getMetricSum(AggTaskMetrics.resultSize)
    val numJobs = appContext.appMetrics.getTotalJobs.max(1)
    val avgResultSize = totalResultSize / numJobs

    val jobSubmissionGaps = getDriverJobSubmissionDelays(
      appContext.jobMap, appContext.stageMap, appContext.stageIDToJobID
    )
    val avgJobDelayMs = if (jobSubmissionGaps.nonEmpty) jobSubmissionGaps.sum / jobSubmissionGaps.size else 0

    val isSchedulingHeavy = avgJobDelayMs > 1000
    val isResultHeavy = avgResultSize > byteStringAsBytes("16mb")

    logger.debug(s"(driver) totalResultSize ${humanReadableBytes(totalResultSize)}")
    logger.debug(s"(driver) numJobs $numJobs")
    logger.debug(s"(driver) avgResultSize ${humanReadableBytes(avgResultSize)}")
    logger.debug(s"(driver) avgJobDelayMs $avgJobDelayMs")
    logger.debug(s"(driver) isResultHeavy $isResultHeavy")
    logger.debug(s"(driver) isSchedulingHeavy $isSchedulingHeavy")

    appContext.appEfficiency.isResultHeavy = isResultHeavy
    appContext.appEfficiency.isSchedulingHeavy = isSchedulingHeavy

    // Executor Metrics
    val executorTotalMemory = appContext.appSparkExecutors.defaultExecutorMemory
    val executorPeakJvmMemory = appContext.appSparkExecutors.getMaxJvmMemoryUsed
    val executorWastedMemory = executorTotalMemory - executorPeakJvmMemory
    val executorSpilledMemory = appContext.appSparkExecutors.getMaxDiskBytesSpilled
    val executorsMaxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed
    val executorsTasksPerSecond: Double = appContext.appMetrics.getTotalTasks / Duration(jobTime, TimeUnit.MILLISECONDS).toSeconds
    val executorCoreMemoryRatio: Long =  appContext.appSparkExecutors.getOnlyExecutors.map(_._2.getCoreMemoryRatio).max

    appContext.appEfficiency.executorTotalMemoryBytes = executorTotalMemory
    appContext.appEfficiency.executorPeakMemoryBytes = executorPeakJvmMemory
    appContext.appEfficiency.executorWastedMemoryBytes = executorWastedMemory
    appContext.appEfficiency.executorSpilledMemoryBytes = executorSpilledMemory
    appContext.appEfficiency.executorsMaxTaskMemory = executorsMaxTaskMemory
    appContext.appEfficiency.executorsTasksPerSecond = executorsTasksPerSecond
    appContext.appEfficiency.executorCoreMemoryRatio = executorCoreMemoryRatio

    val totalCpuTimeNs = appContext.appMetrics.appAggMetrics.getMetricSum(AggTaskMetrics.executorCpuTime)
    val totalRunTimeMs = appContext.appMetrics.appAggMetrics.getMetricSum(AggTaskMetrics.executorRunTime)

    val avgCpuUtil: Double = if (totalRunTimeMs > 0)
      (totalCpuTimeNs.toDouble / (totalRunTimeMs * 1000000)).min(1.0)
    else
      0.0

    val totalGcTime = appContext.appMetrics.appAggMetrics.getMetricSum(AggTaskMetrics.jvmGCTime)
    val totalTasks = appContext.appMetrics.getTotalTasks.max(1)
    val maxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed

    val avgTaskDuration = totalRunTimeMs / totalTasks
    val gcRatio = totalGcTime.toDouble / totalRunTimeMs

    val isCpuBound = avgCpuUtil > 0.7
    val isGcHeavy = gcRatio > 0.1
    val isTaskShort = avgTaskDuration < 2000
    val isTaskMemoryHeavy = maxTaskMemory >= byteStringAsBytes("16g")

    appContext.appEfficiency.isCpuBound = isCpuBound
    appContext.appEfficiency.isGcHeavy = isGcHeavy
    appContext.appEfficiency.isTaskShort = isTaskShort
    appContext.appEfficiency.isTaskMemoryHeavy = isTaskMemoryHeavy

    logger.debug(s"(executor) totalCpuTimeNs     $totalCpuTimeNs")
    logger.debug(s"(executor) totalRunTimeMs     $totalRunTimeMs")
    logger.debug(s"(executor) totalGcTime        $totalGcTime")
    logger.debug(s"(executor) totalTasks         $totalTasks")
    logger.debug(s"(executor) avgTaskDuration    $avgTaskDuration")
    logger.debug(s"(executor) avgCpuUtil         $avgCpuUtil")
    logger.debug(s"(executor) gcRatio            $gcRatio")
    logger.debug(s"(executor) isCpuBound         $isCpuBound")
    logger.debug(s"(executor) isGcHeavy          $isGcHeavy")
    logger.debug(s"(executor) isTaskShort        $isTaskShort")
    logger.debug(s"(executor) isTaskMemoryHeavy  $isTaskMemoryHeavy")

    // ========================================================================
    // Spark Task metrics
    // ========================================================================
    val taskMaxPeakMemory = Try(
      appContext.appMetrics.appAggMetrics.getMetricMax(AggTaskMetrics.peakExecutionMemory)
    ).getOrElse(0L)

    val taskMaxMemorySpilled = Try(
      appContext.appMetrics.appAggMetrics.getMetricMax(AggTaskMetrics.memoryBytesSpilled)
    ).getOrElse(0L)

    val taskMaxDiskSpilled = Try(
      appContext.appMetrics.appAggMetrics.getMetricMax(AggTaskMetrics.diskBytesSpilled)
    ).getOrElse(0L)

    val taskMaxResultSize = Try(
      appContext.appMetrics.appAggMetrics.getMetricMax(AggTaskMetrics.resultSize)
    ).getOrElse(0L)

    appContext.appEfficiency.taskMaxPeakMemory = taskMaxPeakMemory
    appContext.appEfficiency.taskMaxMemorySpilled = taskMaxMemorySpilled
    appContext.appEfficiency.taskMaxDiskSpilled = taskMaxDiskSpilled
    appContext.appEfficiency.taskMaxResultSize = taskMaxResultSize

    logger.debug(s"(task) taskMaxPeakMemory        $taskMaxPeakMemory")
    logger.debug(s"(task) taskMaxMemorySpilled     $taskMaxMemorySpilled")
    logger.debug(s"(task) taskMaxDiskSpilled       $taskMaxDiskSpilled")
    logger.debug(s"(task) taskMaxResultSize        $taskMaxResultSize")

  }

  def getDriverJobSubmissionDelays(
    jobMap: Map[Long, JobTimeSpan],
    stageMap: Map[Int, StageTimeSpan],
    stageIDToJobID: Map[Int, Long]
  ): Seq[Long] = {
    jobMap.flatMap { case (jobId, jobSpan) =>
      val jobSubmissionTime = jobSpan.startTime

      // Find all stage IDs that belong to this job
      val stagesForJob = stageIDToJobID.collect {
        case (stageId, jId) if jId == jobId => stageId
      }

      // Find the earliest stage submission time for this job
      val minStageSubmissionTimeOpt = stagesForJob.flatMap { stageId =>
        stageMap.get(stageId).map(_.startTime)
      }.reduceOption(_ min _)

      // Calculate delay if both job and stage submission time are available
      minStageSubmissionTimeOpt.map { minStageTime =>
        (minStageTime - jobSubmissionTime).max(0L)  // avoid negative due to clock skew
      }
    }.toSeq
  }

}
