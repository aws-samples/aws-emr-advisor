package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.SparkMaxDriverCores
import com.amazonaws.emr.Config.SparkMaxDriverMemory
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.OptimalTypes._
import com.amazonaws.emr.spark.models.runtime.SparkRuntime
import com.amazonaws.emr.spark.scheduler.CompletionEstimator
import com.amazonaws.emr.utils.Constants.ParamDuration
import com.amazonaws.emr.utils.Constants.ParamExecutors
import com.amazonaws.emr.utils.Formatter.asGB
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import com.amazonaws.emr.utils.Formatter.humanReadableBytes
import com.amazonaws.emr.utils.Formatter.roundUp
import org.apache.spark.internal.Logging

import scala.collection.SortedMap
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Try

case class AppRuntimeEstimate(estimatedAppTimeMs: Long,
                              estimatedTotalExecCoreMs: Long
                             )
case class SimulationWithCores(coresPerExecutor: Int,
                               executorNum: Int,
                               appRuntimeEstimate: AppRuntimeEstimate
                                   )
object AppRuntimeEstimate{
  val empty: AppRuntimeEstimate = AppRuntimeEstimate(0L, 0L)
}
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
      appContext.appSparkExecutors.executorsMaxRunning)
    appContext.appRecommendations.currentSparkConf = Some(currentConf)

    val maxExecutors: Int = Try(options(ParamExecutors.name).toInt).getOrElse(Config.ExecutorsMaxTestsCount)
    val expectedDuration: Option[Long] = Try(Duration(options(ParamDuration.name)).toMillis).toOption

    val coresList = getOptimalCoresPerExecutor(appContext)

    if (coresList.isEmpty) {
      throw new RuntimeException("coresList is empty.")
    }

    val simulationList = coresList.flatMap { coreNum =>
      val simulations = estimateRuntime(appContext, coreNum, maxExecutors)
      simulations.map { s =>
        SimulationWithCores(coreNum, s._1, s._2)
      }
    }
    
    // ========================================================================
    // Compute time optimized recommended configurations
    // ========================================================================
    val  timeOptimalSparkConf = getTimeOptimalSparkConf (
      appContext,
      simulationList
    )

    timeOptimalSparkConf.foreach { c =>
      appContext.appRecommendations.sparkConfs.put(TimeOpt, c)
      
      val simulationPageData = simulationList
                          .filter ( _.coresPerExecutor == c.executorCores)
                          .map( s=> s.executorNum -> s.appRuntimeEstimate)
      
      appContext.appRecommendations.executorSimulations = Some(scala.collection.immutable.TreeMap(simulationPageData:_*))
    }
    logInfo("--------------------------runtime optimized------------------------------------------------")
    logInfo(s"${appContext.appRecommendations.sparkConfs.get(TimeOpt)}")
    

    // ========================================================================
    // Compute cost optimized recommended configurations
    // ========================================================================
    getCostOptimalSparkConf(
      appContext,
      simulationList,
      None
    ).foreach (appContext.appRecommendations.sparkConfs.put(CostOpt, _))
    
    logInfo("--------------------------cost optimized------------------------------------------------")
    logInfo(s"${appContext.appRecommendations.sparkConfs.get(CostOpt)}")


    // ========================================================================
    // Compute time capped configurations
    // ========================================================================
    val cappedExecutionTime = expectedDuration.orElse(autoAssignExpectedDuration(appContext.appInfo.duration))
    getCostOptimalSparkConf(appContext,
      simulationList,
      cappedExecutionTime
    ).foreach { c =>
      appContext.appRecommendations.sparkConfs.put(TimeCapped, c)
      cappedExecutionTime.map( t =>
        appContext.appRecommendations.additionalInfo.getOrElseUpdate(TimeCapped, mutable.HashMap()) += (ParamDuration.name -> t.toString)
      )
    }
    logInfo("--------------------------time capped------------------------------------------------")
    logInfo(s"${appContext.appRecommendations.sparkConfs.get(TimeCapped)}")
  }

  private def getTimeOptimalSparkConf(
                                        appContext: AppContext,
                                        simulationList: Seq[SimulationWithCores]
                                     ): Option[SparkRuntime] = {

    val maxDrop: Double = Config.ExecutorsMaxDropLoss
    val SimulationWithCores(optExecutorCores, optExecutorsNum, optEstimatedRuntime) =
      findOptNumExecutorsByTime(simulationList, maxDrop)

    val optExecutorMemoryBytes = findOptExecutorMemory(appContext, optExecutorCores)
    val optExecutorStorageBytes = findOptExecutorStorage(appContext, optExecutorsNum)

    val optCostDriverCores = findOptDriverCores(appContext, optExecutorsNum)
    val optCostDriverMemoryBytes = findOptDriverMemory(appContext)

    logDebug(s"Est. Executor app runtime: ${optEstimatedRuntime})")

    Some(
      SparkRuntime(
        optEstimatedRuntime.estimatedAppTimeMs,
        optCostDriverCores,
        optCostDriverMemoryBytes,
        optExecutorCores,
        optExecutorMemoryBytes,
        optExecutorStorageBytes,
        optExecutorsNum
      ))

  }
  
  // TODO: find this a better algorithm
  private def autoAssignExpectedDuration(x: Long): Option[Long] = {
    Some(x)
  }

  private def getCostOptimalSparkConf(
                                       appContext: AppContext,
                                       simulationList: Seq[SimulationWithCores],
                                       expectedDuration: Option[Long]
                                     ): Option[SparkRuntime] = {

    val maxCostRange: Double = Config.ExecutorsMaxCostRange

    val SimulationWithCores(optCostExecutorCores, optCostExecutorsNum, optCostEstimatedRuntime) =
      findOptNumExecutorsByCost(
        simulationList,
        expectedDuration,
        maxCostRange
      )

    val optCostExecutorMemoryBytes = findOptExecutorMemory(appContext, optCostExecutorCores)
    val optCostExecutorStorageBytes = findOptExecutorStorage(appContext, optCostExecutorsNum)

    val optCostDriverCores = findOptDriverCores(appContext, optCostExecutorsNum)
    val optCostDriverMemoryBytes = findOptDriverMemory(appContext)
    
    logDebug(s"Est. Executor app runtime: ${optCostEstimatedRuntime})")
    
    Some(
      SparkRuntime(
        optCostEstimatedRuntime.estimatedAppTimeMs,
        optCostDriverCores,
        optCostDriverMemoryBytes,
        optCostExecutorCores,
        optCostExecutorMemoryBytes,
        optCostExecutorStorageBytes,
        optCostExecutorsNum
      ))
    
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

  def getOptimalCoresPerExecutor(appContext: AppContext): Seq[Int] = {
    val maxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed
    if (maxTaskMemory >= byteStringAsBytes("16g")) Seq(1)
    else if (maxTaskMemory >= byteStringAsBytes("8g")) Seq(1, 2)
    else if (maxTaskMemory >= byteStringAsBytes("4g")) Seq(2, 4)
    else if (maxTaskMemory >= byteStringAsBytes("2g")) Seq(2, 4, 8)
    else Seq(4, 8, 16)
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
  def estimateRuntime(appContext: AppContext,
    coresPerExecutor: Int,
    maxExecutors: Int): SortedMap[Int, AppRuntimeEstimate] = {
    
    val appRealTime = appContext.appInfo.duration
    
    // TODO: use BO experiments to support large maxExecutors
    val executorsTests = List.range(1, maxExecutors + 1).par

    val simulations = executorsTests
      .map { executorsCount =>
        val (estimatedAppTime, driverTime) = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
          appContext,
          executorsCount,
          coresPerExecutor,
          appRealTime
        )
        val estimatedTotalCoreSeconds = (estimatedAppTime - driverTime) * executorsCount * coresPerExecutor;
        executorsCount -> AppRuntimeEstimate(estimatedAppTime, estimatedTotalCoreSeconds)
      }
      .seq
      .sortBy(_._1)
      .toMap

    SortedMap[Int, AppRuntimeEstimate]() ++ simulations
  }

  /**
   * Compute the number of executors for optimized cost
   *
   * @param data    containing executors count and estimated application time
   * @param maxRange Max time range for filtering the qualified estimate
   */
  def getOptimumCostNumExecutors(data: Seq[SimulationWithCores],
                                 maxRange: Double): SimulationWithCores = {
    val minTotalExecMs = data
      .minBy(_.appRuntimeEstimate.estimatedTotalExecCoreMs)
      .appRuntimeEstimate
      .estimatedTotalExecCoreMs
    
    data
      .filter(
          _.appRuntimeEstimate.estimatedTotalExecCoreMs <= minTotalExecMs * (1 + maxRange)
      )
      .minBy(_.appRuntimeEstimate.estimatedAppTimeMs)
  }

  /**
   * Compute the number of executors for optimized application time using a custom distance function
   *
   * @param data    SortedMap[Int, Long] containing executors count and estimated application time
   * @param maxDrop Max time reduction in percentage to determine when we stop
   */
    
  def getOptimumTimeNumExecutors(data: Seq[SimulationWithCores],
                                 maxDrop: Double): SimulationWithCores = {
    
    val sortedData = data.sortBy(_.executorNum)

    val dropRatios = mutable.SortedMap.empty[Int, Float]
    for (x <- 1 until sortedData.size) {
      val y1 = sortedData(x - 1).appRuntimeEstimate.estimatedAppTimeMs
      val y2 = sortedData(x).appRuntimeEstimate.estimatedAppTimeMs

      // compute drop percentage
      dropRatios(x) = (y1 - y2).toFloat * 100 / y2.toFloat 

    }

    sortedData(
      dropRatios
        .filter(_._2 >= maxDrop)
        .lastOption
        .map(_._1)
        .getOrElse(0)
    )
  }

  /**
   * Find a simulation that matches the requested time. If no run satisfy the requirements,
   * switch to standard `findOptimalNumExecutors` method.
   *
   * @param data         SortedMap[Int, Long] containing executors count and estimated application time
   * @param expectedTime Expected time in milliseconds
   * @param maxDrop      Max time reduction in percentage to determine when we stop
   */
  def findOptNumExecutorsByTime(data: Seq[(SimulationWithCores)],
   maxDrop: Double): SimulationWithCores = {
    
    data
      .groupBy(_.coresPerExecutor)
      .map( d => getOptimumTimeNumExecutors(d._2, maxDrop))
      .minBy(_.appRuntimeEstimate.estimatedAppTimeMs)
  }

  def findOptNumExecutorsByCost(data: Seq[(SimulationWithCores)],
   expectedTime: Option[Long],
   maxRange: Double
  ): SimulationWithCores = {
    expectedTime.map { t =>
      val filtered = data.filter(_.appRuntimeEstimate.estimatedAppTimeMs <= t)
      if (filtered.nonEmpty) getOptimumCostNumExecutors(filtered, maxRange)
      else getOptimumCostNumExecutors(data, maxRange)
    }.getOrElse {
      getOptimumCostNumExecutors(data, maxRange)
    }

  }

}




