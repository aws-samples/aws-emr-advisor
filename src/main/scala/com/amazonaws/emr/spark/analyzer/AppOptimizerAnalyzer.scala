package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.{SparkMaxDriverCores, SparkMaxDriverMemory}
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.runtime.SparkRuntime
import com.amazonaws.emr.spark.scheduler.CompletionEstimator
import com.amazonaws.emr.utils.Constants.{ParamDuration, ParamExecutors}
import com.amazonaws.emr.utils.Formatter.{asGB, byteStringAsBytes, humanReadableBytes, printDuration, roundUp}
import org.apache.spark.internal.Logging

import scala.collection.{SortedMap, mutable}
import scala.concurrent.duration.Duration
import scala.util.Try

class AppOptimizerAnalyzer extends AppAnalyzer with Logging {

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logInfo("Analyze Spark settings...")

    // ========================================================================
    // Current configurations
    // ========================================================================
    val currentConf = SparkRuntime(
      appContext.appInfo.duration,
      appContext.appSparkExecutors.defaultDriverCores,
      appContext.appSparkExecutors.defaultDriverMemory,
      appContext.appSparkExecutors.defaultExecutorCores,
      appContext.appSparkExecutors.defaultExecutorMemory,
      appContext.appSparkExecutors.getRequiredStoragePerExecutor,
      appContext.appSparkExecutors.executorsMaxRunning
    )
    appContext.appRecommendations.currentSparkConf = Some(currentConf)

    // ========================================================================
    // Compute recommended configurations
    // ========================================================================
    val optExecutorCores = findOptimalExecutorCores(appContext)
    val optExecutorMemoryBytes = findOptExecutorMemory(appContext, optExecutorCores)

    val maxExecutors: Int = Try(options(ParamExecutors.name).toInt).getOrElse(Config.ExecutorsMaxTestsCount)
    val expectedDuration: Long = Try(Duration(options(ParamDuration.name)).toMillis).getOrElse(Long.MaxValue)

    val simulations = estimateRuntime(appContext, optExecutorCores, maxExecutors)
    val (optExecutorsNum, estimatedRuntime) = findOptNumExecutorsByTime(simulations, expectedDuration)
    val optExecutorStorageBytes = findOptExecutorStorage(appContext, optExecutorsNum)

    val optDriverCores = findOptDriverCores(appContext, optExecutorsNum)
    val optDriverMemoryBytes = findOptDriverMemory(appContext)

    appContext.appRecommendations.executorSimulations = Some(simulations)
    appContext.appRecommendations.optimalSparkConf = Some(
      SparkRuntime(
        estimatedRuntime,
        optDriverCores,
        optDriverMemoryBytes,
        optExecutorCores,
        optExecutorMemoryBytes,
        optExecutorStorageBytes,
        optExecutorsNum
      ))

    logDebug("--------------------------------------------------------------------------")
    logDebug(s" - Est. Driver Cores: $optDriverCores (${currentConf.driverCores})")
    logDebug(s" - Est. Driver Memory: ${humanReadableBytes(optDriverMemoryBytes)} (${humanReadableBytes(currentConf.driverMemory)})")
    logDebug(s" - Est. Executor Cores: $optExecutorCores (${currentConf.executorCores})")
    logDebug(s" - Est. Executor Memory: ${humanReadableBytes(optExecutorMemoryBytes)} (${humanReadableBytes(currentConf.executorMemory)})")
    logDebug(s" - Est. Executor Storage: ${humanReadableBytes(optExecutorStorageBytes)} (${humanReadableBytes(currentConf.executorStorageRequired)})")
    logDebug(s" - Est. Executor Number: $optExecutorsNum (${currentConf.executorsNum})")
    logDebug(s" - Est. Application Runtime: ${printDuration(estimatedRuntime)} (${printDuration(currentConf.runtime)})")
    logDebug("--------------------------------------------------------------------------")

  }


  /**
   * Estimate the driver cores, using the number of executors and evaluating
   * the time spent by the application running on the driver.
   */
  def findOptDriverCores(appContext: AppContext, numExecutors: Int): Int = {
    // we can assume that if we spend too much time on the driver,
    // we'll need some extra compute power
    val cores = if (appContext.appEfficiency.driverTimePercentage > 30) SparkMaxDriverCores
    else (numExecutors / 100).max(1).min(SparkMaxDriverCores)

    cores
  }

  def findOptDriverMemory(appContext: AppContext): Long = {
    val sparkResultSize = appContext.appMetrics.appAggMetrics.getMetricSum(AggTaskMetrics.resultSize)
    val estDriverMemoryGb = roundUp(asGB(sparkResultSize)) + 1
    val estDriverMemoryBytes = byteStringAsBytes(s"${estDriverMemoryGb}gb").min(byteStringAsBytes(SparkMaxDriverMemory))

    logDebug(s" - Driver Usage: ${humanReadableBytes(sparkResultSize)} (Spark Result Size)")

    estDriverMemoryBytes
  }

  /**
   * Simple logic to be revised
   * - Use Spark Task time to determine if we can over-subscribe resources
   */
  def findOptimalExecutorCores(appContext: AppContext): Int = {
    val maxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed
    if (maxTaskMemory >= byteStringAsBytes("16g")) 1
    else if (maxTaskMemory >= byteStringAsBytes("8g")) 2
    else if (maxTaskMemory >= byteStringAsBytes("1g")) 4
    else if (maxTaskMemory >= byteStringAsBytes("500m")) 8
    else 16
  }

  /**
   * Compute the "optimal" executor memory given a pre-defined number of Cores per Executor.
   * The memory is computed in the following way:
   *
   * - We use two main metrics. Maximum Task Memory and Max JVM Heap memory
   * - We compute the memory ratios for these values and we get the max cross the two
   * - We round the memory to the next GB before returning it
   * - We do not consider explicitly any Storage Memory. Calculation basically infer execution memory to use
   * also the full storage memory.
   *
   * This is a simple way to compute the executor memory, and this logic should be refined.
   */
  def findOptExecutorMemory(appContext: AppContext, optimalExecutorCores: Int): Long = {

    val oldExecutorCores = appContext.appSparkExecutors.defaultExecutorCores

    val estJvmMemoryPerCore = appContext.appSparkExecutors.getMaxJvmMemoryUsed / oldExecutorCores
    val maxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed

    val recommendedMemoryPerCore = estJvmMemoryPerCore.max(maxTaskMemory)
    val minSparkExecutionMemory = recommendedMemoryPerCore * optimalExecutorCores

    val SparkReservedMemoryBytes = 300 * 1024 * 1024L
    val memoryGb = asGB(minSparkExecutionMemory + SparkReservedMemoryBytes)
    val recommendedMemoryBytes = byteStringAsBytes(s"${roundUp(memoryGb)}g")

    recommendedMemoryBytes
  }

  /**
   * Compute the optimal storage in bytes per single executor.
   * When computing the storage requirements, we take into account:
   *
   * - use different logic if writes are uniform or not
   * - the data spilled from memory to disks
   * - the shuffle data written on the disks
   * - an extra 20% of space based on the computed one to take into account variation in the data
   * - we round the final computed value to the closest GB value
   */
  def findOptExecutorStorage(appContext: AppContext, executorCount: Int): Long = {
    if (appContext.appSparkExecutors.isShuffleWriteUniform) findOptExecutorStorageUniform(appContext, executorCount)
    else findOptExecutorStorageNonUniform(appContext)
  }

  /**
   * Compute the optimal storage in bytes per single executor when writes are uniform.
   *
   * @param appContext    Spark application data
   * @param executorCount Expected number of concurrent executors
   */
  def findOptExecutorStorageUniform(appContext: AppContext, executorCount: Int): Long = {
    val totalShuffleBytes = appContext.appSparkExecutors.getTotalShuffleBytesWritten
    val totalSpilledBytes = appContext.appSparkExecutors.getTotalDiskBytesSpilled
    val singleExecutorStorage = (totalShuffleBytes + totalSpilledBytes) / executorCount
    computeStorage(singleExecutorStorage)
  }

  /**
   * Compute the optimal storage in bytes per single executor when writes are not uniform.
   *
   * @param appContext Spark application data
   */
  def findOptExecutorStorageNonUniform(appContext: AppContext): Long = {
    val maxShuffleWrite = appContext.appSparkExecutors.executors.values.map(x =>
      Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.shuffleWriteBytesWritten)
      ).getOrElse(0L)).max
    val maxDiskSpilledWrite = appContext.appSparkExecutors.executors.values.map(x =>
      Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.diskBytesSpilled)
      ).getOrElse(0L)).max
    val singleExecutorStorage = maxShuffleWrite + maxDiskSpilledWrite
    computeStorage(singleExecutorStorage)
  }

  private def computeStorage(bytesWritten: Long): Long = {
    val extraStorage = bytesWritten * 20 / 100
    val totalStorageGb = asGB(bytesWritten + extraStorage)
    val roundedStorageGb = roundUp(totalStorageGb)
    byteStringAsBytes(s"${roundedStorageGb}gb")
  }

  /**
   * Estimate application runtime using different executors counts.
   *
   * @param appContext       Spark application data
   * @param coresPerExecutor Number of executors cores
   */
  def estimateRuntime
  (appContext: AppContext,
    coresPerExecutor: Int,
    maxExecutors: Int = Config.ExecutorsMaxTestsCount): SortedMap[Int, Long] = {
    val appRealTime = appContext.appInfo.duration
    val executorsTests = List.range(1, maxExecutors).par

    val simulations = executorsTests
      .map { executorsCount =>
        val estimatedAppTime = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
          appContext,
          executorsCount,
          coresPerExecutor,
          appRealTime
        )
        executorsCount -> estimatedAppTime
      }
      .seq
      .sortBy(_._1)
      .toMap

    SortedMap[Int, Long]() ++ simulations
  }

  /**
   * Compute the number of executors using a custom distance function
   *
   * @param data    SortedMap[Int, Long] containing executors count and estimated application time
   * @param maxDrop Max time reduction in percentage to determine when we stop
   */
  def findOptNumExecutors(data: SortedMap[Int, Long], maxDrop: Double = Config.ExecutorsMaxDropLoss): (Int, Long) = {

    val distance = mutable.SortedMap.empty[Int, Float]
    for (x <- 1 until data.size) {
      val y1 = data.getOrElse(x, 0L).toFloat
      val y2 = data.getOrElse(x + 1, 0L).toFloat
      // compute drop percentage
      distance(x) = (y1 - y2) * 100 / y1
    }

    Try {
      val minExec = distance.filter(x => x._2 >= maxDrop).maxBy(_._1)
      data.find(_._1 == (minExec._1 + 1)).getOrElse(data.head)
    }.getOrElse(data.head)
  }

  /**
   * Find a simulation that matches the requested time. If no run satisfy the requirements,
   * switch to standard `findOptimalNumExecutors` method.
   *
   * @param data         SortedMap[Int, Long] containing executors count and estimated application time
   * @param expectedTime Expected time in milliseconds
   * @param maxDrop      Max time reduction in percentage to determine when we stop
   */
  def findOptNumExecutorsByTime
  (data: SortedMap[Int, Long],
    expectedTime: Long,
    maxDrop: Double = Config.ExecutorsMaxDropLoss): (Int, Long) = {

    val filtered = data.filter(_._2 >= expectedTime)
    if (filtered.nonEmpty) filtered.last
    else findOptNumExecutors(data, maxDrop)
  }

}




