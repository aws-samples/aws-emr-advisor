package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config.{SparkMaxDriverCores, SparkMaxDriverMemory}
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.spark.models.runtime.SparkRuntime
import com.amazonaws.emr.utils.Formatter.{asGB, byteStringAsBytes, roundUp}
import org.apache.logging.log4j.scala.Logging

import scala.util.Try

object SparkBaseOptimizer extends Logging {

  private val DefaultReservedMemoryBytes = 300 * 1024 * 1024L
  private val DefaultExtraStorageFactor = 0.2

  def createSparkRuntime(appContext: AppContext, sim: SimulationWithCores): SparkRuntime = {
    val executorMemory = SparkBaseOptimizer.findOptExecutorMemory(appContext, sim.coresPerExecutor)
    val executorStorage = SparkBaseOptimizer.findOptExecutorStorage(appContext, sim.executorNum)
    val driverCores = SparkBaseOptimizer.findOptDriverCores(appContext, sim.executorNum)
    val driverMemory = SparkBaseOptimizer.findOptDriverMemory(appContext)

    SparkRuntime(
      sim.appRuntimeEstimate.estimatedAppTimeMs,
      driverCores,
      driverMemory,
      sim.coresPerExecutor,
      executorMemory,
      executorStorage,
      sim.executorNum
    )
  }

  /**
   * Finds the optimal number of driver cores based on application efficiency and executor count.
   *
   * @param appContext   The context containing application metrics and configurations.
   * @param numExecutors The number of executors currently in use by the application.
   * @return The recommended number of cores for the Spark driver.
   */
  def findOptDriverCores(appContext: AppContext, numExecutors: Int): Int = {
    val driverTime = appContext.appEfficiency.driverTimePercentage
    if (driverTime > 30) SparkMaxDriverCores
    else (numExecutors / 100).max(1).min(SparkMaxDriverCores)
  }

  /**
   * Estimates the optimal memory required for the Spark driver based on application metrics.
   *
   * @param appContext The context containing application metrics and configurations.
   * @return The recommended driver memory in bytes.
   */
  def findOptDriverMemory(appContext: AppContext): Long = {
    val sparkResultSize = appContext.appMetrics.appAggMetrics.getMetricSum(AggTaskMetrics.resultSize)
    val estimatedMemoryGb = roundUp(asGB(sparkResultSize)) + 1
    byteStringAsBytes(s"${estimatedMemoryGb}gb").min(byteStringAsBytes(SparkMaxDriverMemory))
  }

  /**
   * Suggests the optimal number of cores per executor based on maximum task memory usage.
   *
   * @param appContext The context containing application metrics and configurations.
   * @return A sequence of recommended core counts per executor.
   */
  def getOptimalCoresPerExecutor(appContext: AppContext): Seq[Int] = {
    val maxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed

    maxTaskMemory match {
      case memory if memory >= byteStringAsBytes("16g") => Seq(1)
      case memory if memory >= byteStringAsBytes("8g")  => Seq(1, 2)
      case memory if memory >= byteStringAsBytes("4g")  => Seq(1, 2, 4)
      case memory if memory >= byteStringAsBytes("2g")  => Seq(1, 2, 4, 8)
      case _                                            => Seq(1, 2, 4, 8, 16)
    }
  }

  /**
   * Calculates the optimal executor memory based on JVM and task memory usage.
   *
   * @param appContext           The context containing application metrics and configurations.
   * @param optimalExecutorCores The recommended number of cores per executor.
   * @return The recommended executor memory in bytes.
   */
  def findOptExecutorMemory(appContext: AppContext, optimalExecutorCores: Int): Long = {
    val oldExecutorCores = appContext.appSparkExecutors.defaultExecutorCores
    val estJvmMemoryPerCore = appContext.appSparkExecutors.getMaxJvmMemoryUsed / oldExecutorCores
    val maxTaskMemory = appContext.appSparkExecutors.getMaxTaskMemoryUsed

    val recommendedMemoryPerCore = estJvmMemoryPerCore.max(maxTaskMemory)
    val minSparkExecutionMemory = recommendedMemoryPerCore * optimalExecutorCores + DefaultReservedMemoryBytes

    byteStringAsBytes(s"${roundUp(asGB(minSparkExecutionMemory))}g")
  }

  /**
   * Determines the optimal storage per executor based on shuffle and spill metrics.
   *
   * @param appContext   The context containing application metrics and configurations.
   * @param executorCount The expected number of concurrent executors.
   * @return The recommended storage in bytes per executor.
   */
  def findOptExecutorStorage(appContext: AppContext, executorCount: Int): Long = {
    if (appContext.appSparkExecutors.isShuffleWriteUniform) {
      findOptExecutorStorageUniform(appContext, executorCount)
    } else {
      findOptExecutorStorageNonUniform(appContext)
    }
  }

  /**
   * Computes the optimal storage per executor for uniform data distribution.
   *
   * @param appContext   The context containing application metrics and configurations.
   * @param executorCount The expected number of concurrent executors.
   * @return The recommended storage in bytes per executor.
   */
  private def findOptExecutorStorageUniform(appContext: AppContext, executorCount: Int): Long = {
    val totalShuffleBytes = appContext.appSparkExecutors.getTotalShuffleBytesWritten
    val totalSpilledBytes = appContext.appSparkExecutors.getTotalDiskBytesSpilled
    val singleExecutorStorage = (totalShuffleBytes + totalSpilledBytes) / executorCount
    computeStorage(singleExecutorStorage)
  }

  /**
   * Computes the optimal storage per executor for non-uniform data distribution.
   *
   * @param appContext The context containing application metrics and configurations.
   * @return The recommended storage in bytes per executor.
   */
  private def findOptExecutorStorageNonUniform(appContext: AppContext): Long = {
    val maxShuffleWrite = appContext.appSparkExecutors.executors.values.map(x =>
      Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.shuffleWriteBytesWritten)
      ).getOrElse(0L)).max
    val maxDiskSpilledWrite = appContext.appSparkExecutors.executors.values.map(x =>
      Try(x.executorTaskMetrics.getMetricSum(AggTaskMetrics.diskBytesSpilled)
      ).getOrElse(0L)).max
    val singleExecutorStorage = maxShuffleWrite + maxDiskSpilledWrite
    computeStorage(singleExecutorStorage)
  }

  /**
   * Computes the total storage required, adding an extra buffer for data variation.
   *
   * @param bytesWritten The total bytes written (shuffle + spill).
   * @return The recommended storage in bytes, rounded to the nearest GB.
   */
  private def computeStorage(bytesWritten: Long): Long = {
    val extraStorage = (bytesWritten * DefaultExtraStorageFactor).toLong
    byteStringAsBytes(s"${roundUp(asGB(bytesWritten + extraStorage))}gb")
  }

}
