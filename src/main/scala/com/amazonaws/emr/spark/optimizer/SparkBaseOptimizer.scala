package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config.{SparkMaxDriverCores, SparkMaxDriverMemory}
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.WorkerSupportedConfig
import com.amazonaws.emr.spark.models.runtime.SparkRuntime
import com.amazonaws.emr.utils.Formatter.{asGB, byteStringAsBytes, roundUp}
import org.apache.logging.log4j.scala.Logging

import scala.util.Try

/**
 * SparkBaseOptimizer provides functions to compute optimal Spark driver and executor configurations
 * (CPU cores, memory, and storage) based on application-level metrics and runtime characteristics.
 *
 * This optimizer supports both EC2 and EMR Serverless environments, using empirical data and simulation
 * results.
 */
object SparkBaseOptimizer extends Logging {

  private val DefaultReservedMemoryBytes = 300 * 1024 * 1024L
  private val DefaultExtraStorageFactor = 0.2

  /**
   * Generates a SparkRuntime configuration optimized for EC2-based Spark clusters.
   *
   * This method combines simulated runtime characteristics (from parallelism simulations) with
   * workload-aware executor and driver settings derived from historical metrics. It assumes the target
   * environment is EC2 with YARN and non-dynamic allocation.
   *
   * The final SparkRuntime includes:
   *   - Executor memory: Based on peak task memory, spills, and result size
   *   - Executor storage: Estimated from shuffle and disk spill patterns
   *   - Driver memory: Derived from result size, scheduling delay, and execution memory
   *   - Core counts: Based on CPU vs. memory pressure indicators
   *
   * @param appContext Application context containing workload and efficiency data
   * @param sim        Simulation result specifying executor core count and number of executors
   * @return A fully-formed SparkRuntime config with optimal memory, cores, and storage settings
   */
  def createEc2SparkRuntime(appContext: AppContext, sim: SimulationWithCores): SparkRuntime = {
    val executorMemory = SparkBaseOptimizer.findOptExecutorMemory(appContext, sim.coresPerExecutor)
    val executorStorage = SparkBaseOptimizer.findOptExecutorStorage(appContext, sim.executorNum)
    val driverCores = SparkBaseOptimizer.findOptDriverCores(appContext, sim.executorNum)
    val driverMemory = SparkBaseOptimizer.findOptDriverMemory(appContext)

    SparkRuntime(
      runtime = sim.appRuntimeEstimate.estimatedAppTimeMs,
      driverCores = driverCores,
      driverMemory = driverMemory,
      executorCores = sim.coresPerExecutor,
      executorMemory = executorMemory,
      executorStorageRequired = executorStorage,
      executorsNum = sim.executorNum
    )
  }

  /**
   * Constructs an optimized SparkRuntime configuration compatible with EMR Serverless constraints.
   *
   * This method ensures that CPU and memory configurations align with valid Serverless worker sizes.
   * It selects the closest valid worker tier based on desired cores and memory, adjusting within bounds
   * as needed. If no matching configuration is found, returns None.
   *
   * Requirements enforced:
   *   - Cores must match a supported EMR Serverless worker size
   *   - Memory must fall within [min, max] for that core tier
   *   - Memory must be expressed in whole GBs
   *
   * @param appContext The context containing workload and resource metrics
   * @param sim        Simulated runtime configuration (cores/executors)
   * @return An Option[SparkRuntime] if valid, otherwise None
   */
  def createSvlSparkRuntime(appContext: AppContext, sim: SimulationWithCores): Option[SparkRuntime] = {
    val executorMemory = SparkBaseOptimizer.findOptExecutorMemory(appContext, sim.coresPerExecutor)
    val executorStorage = SparkBaseOptimizer.findOptExecutorStorage(appContext, sim.executorNum)
    val driverCores = SparkBaseOptimizer.findOptDriverCores(appContext, sim.executorNum)
    val driverMemory = SparkBaseOptimizer.findOptDriverMemory(appContext)

    val driverWorkerOpt = WorkerSupportedConfig.find(_.cpu == driverCores)
    val execWorkerOpt = WorkerSupportedConfig.find(_.cpu == sim.coresPerExecutor)

    if (execWorkerOpt.isEmpty || driverWorkerOpt.isEmpty) return None

    val execWorker = execWorkerOpt.get
    val driverWorker = driverWorkerOpt.get

    val execMemoryGb = asGB(executorMemory).toInt
    val driverMemoryGb = asGB(driverMemory).toInt

    val boundedExecMemGb = execMemoryGb.max(execWorker.minMemoryGB).min(execWorker.maxMemoryGB)
    val boundedDriverMemGb = driverMemoryGb.max(driverWorker.minMemoryGB).min(driverWorker.maxMemoryGB)

    Some(
      SparkRuntime(
        runtime = sim.appRuntimeEstimate.estimatedAppTimeMs,
        driverCores = driverWorker.cpu,
        driverMemory = byteStringAsBytes(s"${boundedDriverMemGb}g"),
        executorCores = execWorker.cpu,
        executorMemory = byteStringAsBytes(s"${boundedExecMemGb}g"),
        executorStorageRequired = executorStorage,
        executorsNum = sim.executorNum
      )
    )
  }

  /**
   * Determines the optimal number of driver CPU cores.
   *
   * If the application is identified as scheduling or result heavy, it uses the max allowed driver cores.
   * Otherwise, it scales based on executor count (min 2), capped by SparkMaxDriverCores.
   *
   * @param appContext    The context containing application efficiency flags.
   * @param executorCount The number of executors in the simulated or observed application.
   * @return The recommended number of CPU cores for the Spark driver.
   */
  def findOptDriverCores(appContext: AppContext, executorCount: Int): Int = {
    val schedulingHeavy = appContext.appEfficiency.isSchedulingHeavy
    val resultHeavy = appContext.appEfficiency.isResultHeavy

    val baseCores = if (schedulingHeavy || resultHeavy) SparkMaxDriverCores else (executorCount / 100).max(2)
    baseCores.min(SparkMaxDriverCores)
  }

  /**
   * Estimates the optimal memory required for the Spark driver.
   *
   * Combines result size and peak execution memory, adds a buffer, and ensures it does not exceed
   * SparkMaxDriverMemory.
   *
   * @param appContext The context containing aggregate task metrics.
   * @return The recommended driver memory in bytes.
   */
  def findOptDriverMemory(appContext: AppContext): Long = {
    val resultSize = appContext.appMetrics.appAggMetrics.getMetricMax(AggTaskMetrics.resultSize)
    val peakExecMemory = appContext.appMetrics.appAggMetrics.getMetricMax(AggTaskMetrics.peakExecutionMemory)

    val rawEstimate = resultSize + peakExecMemory
    val withBuffer = (rawEstimate * 1.1).toLong

    val roundedUp = byteStringAsBytes(s"${roundUp(asGB(withBuffer)) + 1}g")
    roundedUp.min(byteStringAsBytes(SparkMaxDriverMemory))
  }

  /**
   * Recommends a set of optimal executor core counts based on workload characteristics.
   *
   * Core count selection impacts parallelism, GC behavior, and memory pressure:
   *
   * - For memory-heavy or GC-heavy tasks: fewer cores per executor help avoid OOMs and reduce GC pause times.
   * - For CPU-bound and short-lived tasks: higher core counts improve throughput and resource efficiency.
   *
   * The logic evaluates four key workload traits:
   *   - isTaskMemoryHeavy: tasks consume a lot of memory
   *   - isGcHeavy: JVM spends significant time in garbage collection
   *   - isCpuBound: tasks fully utilize CPU during execution
   *   - isTaskShort: tasks finish quickly (often < 2 sec)
   *
   * @param appContext Application context with workload behavior classification
   * @return A sequence of recommended executor core counts (e.g., 2, 4, 8)
   */
  def findOptCoresPerExecutor(appContext: AppContext): Seq[Int] = {
    val isShortTasks = appContext.appEfficiency.isTaskShort
    val isGcHeavy = appContext.appEfficiency.isGcHeavy
    val isCpuBound = appContext.appEfficiency.isCpuBound
    val isTaskMemoryHeavy = appContext.appEfficiency.isTaskMemoryHeavy

    val suggestions: Seq[Int] = (isCpuBound, isGcHeavy, isShortTasks, isTaskMemoryHeavy) match {
      case (_, _, _, true)        => Seq(1, 2, 4)   // prioritize fewer cores for memory safety
      case (_, true, _, _)        => Seq(1, 2, 4)   // reduce GC amplification per executor
      case (true, false, true, _) => Seq(4, 8, 16)  // CPU-intensive + fast tasks = go wide
      case _                      => Seq(2, 4, 8)   // balanced/default
    }

    if (suggestions.nonEmpty) suggestions else Seq(4)
  }

  /**
   * Estimates optimal executor memory based on per-task peak usage and memory pressure indicators.
   *
   * This function uses the maximum values observed across all tasks for:
   * - Peak execution memory (on-heap structures)
   * - Memory spilled to disk
   * - Serialized result size returned to the driver
   *
   * It also includes a buffer to absorb partial spill data and adds a static reserved memory buffer.
   *
   * Memory is scaled per core, rounded to the next GB, and capped by a safe minimum.
   *
   * @param appContext     Application metrics context
   * @param executorCores  Number of cores per executor
   * @return Optimal executor memory in bytes
   */
  def findOptExecutorMemory(appContext: AppContext, executorCores: Int): Long = {
    val taskMaxPeakMemory = appContext.appEfficiency.taskMaxPeakMemory
    val taskMaxMemorySpilled = appContext.appEfficiency.taskMaxMemorySpilled
    val taskMaxDiskSpilled = appContext.appEfficiency.taskMaxDiskSpilled
    val taskMaxResultSize = appContext.appEfficiency.taskMaxResultSize

    val DiskSpillCompensationRatio = 0.25
    val diskSpillBuffer = taskMaxDiskSpilled * DiskSpillCompensationRatio

    val spillAndResultBuffer = taskMaxResultSize + diskSpillBuffer.toLong
    val totalMemoryPerCore = taskMaxPeakMemory + taskMaxMemorySpilled + spillAndResultBuffer

    val totalMemory = totalMemoryPerCore * executorCores + DefaultReservedMemoryBytes
    val rounded = byteStringAsBytes(s"${roundUp(asGB(totalMemory))}g")

    math.max(rounded, byteStringAsBytes("2g"))
  }

  /**
   * Determines the optimal storage per executor based on shuffle write patterns.
   *
   * Uses uniform or non-uniform logic depending on observed shuffle write behavior across executors.
   *
   * @param appContext    The context containing executor-level shuffle and spill metrics.
   * @param executorCount The number of concurrent executors expected to run.
   * @return The recommended local storage per executor in bytes.
   */
  def findOptExecutorStorage(appContext: AppContext, executorCount: Int): Long = {
    if (appContext.appSparkExecutors.isShuffleWriteUniform) {
      findOptExecutorStorageUniform(appContext, executorCount)
    } else {
      findOptExecutorStorageNonUniform(appContext)
    }
  }

  /**
   * Computes optimal executor storage assuming uniform shuffle write distribution.
   *
   * @param appContext    The context with application executor metrics.
   * @param executorCount Number of expected concurrent executors.
   * @return Recommended local storage per executor in bytes.
   */
  private def findOptExecutorStorageUniform(appContext: AppContext, executorCount: Int): Long = {
    val totalShuffleBytes = appContext.appSparkExecutors.getTotalShuffleBytesWritten
    val totalSpilledBytes = appContext.appSparkExecutors.getTotalDiskBytesSpilled
    val singleExecutorStorage = (totalShuffleBytes + totalSpilledBytes) / executorCount
    computeStorage(singleExecutorStorage)
  }

  /**
   * Computes optimal executor storage for skewed or non-uniform shuffle write patterns.
   *
   * Finds the maximum shuffle write and disk spill metrics across all executors.
   *
   * @param appContext The context with executor-level metrics.
   * @return Recommended local storage per executor in bytes.
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
   * Adds a safety buffer to account for data size variation and rounds up to the nearest GB.
   *
   * Used to calculate final storage recommendation.
   *
   * @param bytesWritten Raw shuffle and spill bytes written.
   * @return Final storage size in bytes.
   */
  private def computeStorage(bytesWritten: Long): Long = {
    val extraStorage = (bytesWritten * DefaultExtraStorageFactor).toLong
    byteStringAsBytes(s"${roundUp(asGB(bytesWritten + extraStorage))}gb")
  }

}
