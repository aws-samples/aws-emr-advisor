package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.spark.scheduler.JobOverlapHelper
import org.apache.spark.internal.Logging

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class AppEfficiencyAnalyzer extends AppAnalyzer with Logging {

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logInfo("Analyze efficiency...")

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

    //sum of millis used by all tasks of all jobs
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
  }

}
