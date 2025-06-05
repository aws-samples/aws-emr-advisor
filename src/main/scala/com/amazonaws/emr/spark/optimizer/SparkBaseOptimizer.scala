package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config.{SparkExecutorCoresBalanced, SparkExecutorCoresCpuIntensive, SparkExecutorCoresMemoryIntensive, SparkMaxDriverCores, SparkMaxDriverMemory, SparkStageMaxMemorySpill}
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics
import com.amazonaws.emr.utils.Formatter.{asGB, byteStringAsBytes, humanReadableBytes, roundUp}
import org.apache.logging.log4j.scala.Logging

import scala.util.Try

/**
 * SparkBaseOptimizer provides functions to compute optimal Spark driver and executor configurations
 * (CPU cores, memory, and storage) based on application-level metrics and runtime characteristics.
 *
 * This optimizer supports both EC2 and EMR Serverless environments, using empirical data and simulation
 * results.
 */
class SparkBaseOptimizer(val appContext: AppContext) extends Logging {

  private val DefaultExtraStorageFactor = 0.2
  private val DefaultReservedMemoryBytes = 300 * 1024 * 1024L

  /**
   * Determines the optimal number of driver CPU cores.
   *
   * If the application is identified as scheduling or result heavy, it uses the max allowed driver cores.
   * Otherwise, it scales based on executor count (min 2), capped by SparkMaxDriverCores.
   *
   * @param executorCount The number of executors in the simulated or observed application.
   * @return The recommended number of CPU cores for the Spark driver.
   */
  def recommendDriverCores(executorCount: Int): Int = {
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
   * @return The recommended driver memory in bytes.
   */
  def recommendDriverMemory(): Long = {
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
   * @return A sequence of recommended executor core counts (e.g., 2, 4, 8)
   */
  def recommendExecutorCores(): Seq[Int] = {
    val isShortTasks = appContext.appEfficiency.isTaskShort
    val isGcHeavy = appContext.appEfficiency.isGcHeavy
    val isCpuBound = appContext.appEfficiency.isCpuBound
    val isTaskMemoryHeavy = appContext.appEfficiency.isTaskMemoryHeavy

    (isCpuBound, isGcHeavy, isShortTasks, isTaskMemoryHeavy) match {
      case (_, _, _, true)        => SparkExecutorCoresMemoryIntensive  // prioritize fewer cores for memory safety
      case (_, true, _, _)        => SparkExecutorCoresMemoryIntensive  // reduce GC amplification per executor
      case (true, _, true, _)     => SparkExecutorCoresCpuIntensive     // CPU-intensive + fast tasks = go wide
      case _                      => SparkExecutorCoresBalanced         // balanced / default
    }
  }

  /**
   * Recommends optimal Spark executor memory based on the application context metrics.
   *
   * This method calculates executor memory considering:
   * - Peak task memory usage from historical stages.
   * - Spill to disk (memory pressure indicators).
   * - Serialized task results returned to the driver.
   * - Reserved memory buffer for Spark overhead and JVM operation.
   *
   * It ensures memory allocation is rounded up to the nearest gigabyte, with a minimum of 2GB per executor.
   *
   * @param executorCores Number of cores per executor
   * @param executorsNum Number of executors
   * @return Recommended executor memory in bytes, rounded to the nearest gigabyte.
   */
  def recommendExecutorMemory(executorCores: Int, executorsNum: Int): Long = {
    val eff = appContext.appEfficiency
    val stageMetrics = eff.stageSummaryMetrics
    val maxTasksConcurrent = executorCores * executorsNum

    val memoryRequirements = stageMetrics.map { stage =>
      val batches = math.ceil(stage.numberOfTasks / maxTasksConcurrent).toInt
      val estimatedMemorySpilled = (stage.stageAvgMemorySpilled * executorCores) * batches
      (stage.id, stage.stageAvgPeakMemory, stage.stageAvgMemorySpilled, estimatedMemorySpilled)
    }

    // Select the most memory-intensive stage
    val (stageId, stageAvgPeakMemory, stageAvgMemorySpilled, maxMemorySpilledPerExecutor) = memoryRequirements.maxBy(_._2)

    // Spill and result size considerations - We only consider 20% of the data spilled
    val heavySpill = maxMemorySpilledPerExecutor >= byteStringAsBytes(SparkStageMaxMemorySpill)
    val spillBuffer = if (heavySpill) (stageAvgMemorySpilled * 0.2).toLong else 0L
    val resultBuffer = eff.taskMaxResultSize

    // Clearly separate calculation for clarity
    val totalMemoryPerCore = stageAvgPeakMemory + resultBuffer + spillBuffer
    val totalExecutorMemory = totalMemoryPerCore * executorCores + DefaultReservedMemoryBytes

    val roundedMemoryGB = roundUp(asGB(totalExecutorMemory))
    val roundedMemoryBytes = byteStringAsBytes(s"${roundedMemoryGB}g")
    val finalExecutorMemory = math.max(roundedMemoryBytes, byteStringAsBytes("2g"))

    //println("============= EXECUTOR MEMORY RECOMMENDATION =============")
    //println(s"Stage with max memory (Stage ID) : $stageId")
    //println(s"Executor cores                   : $executorCores")
    //println(s"Number of Executors              : $executorsNum")
    //println(s"Avg. Peak memory per task        : ${humanReadableBytes(stageAvgPeakMemory)}")
    //println(s"Avg. Spilled memory per task     : ${humanReadableBytes(stageAvgMemorySpilled)}")
    //println(s"Spill buffer                     : ${humanReadableBytes(spillBuffer)}")
    //println(s"Result buffer                    : ${humanReadableBytes(resultBuffer)}")
    //println(s"Total memory per core            : ${humanReadableBytes(totalMemoryPerCore)}")
    //println(s"Recommended Executor Memory      : ${humanReadableBytes(finalExecutorMemory)}")
    //println("==========================================================")

    finalExecutorMemory
  }

  /**
   * Selects the optimal number of executors from a set of simulation results based on runtime improvement.
   *
   * This method analyzes the relative runtime reductions between consecutive executor configurations.
   * It identifies the point where additional executors no longer yield a significant performance gain
   * (as defined by the `maxDrop` threshold), and returns the configuration just before the benefit drops off.
   *
   * The logic works by:
   *   - Sorting the simulation data by number of executors.
   *   - Calculating the percentage drop in runtime between each pair of consecutive configurations.
   *   - Finding the last point where the percentage drop exceeds or equals the given threshold.
   *   - Returning the configuration at that point (or the first if no significant drop is found).
   *
   * @param simulations A sequence of `SimulationWithCores`, each representing estimated runtime for a given executor count.
   * @param maxDrop The minimum percentage improvement in runtime required to justify adding more executors.
   * @return The simulation configuration that offers the best tradeoff between executor count and runtime improvement.
   */
  def recommendExecutorNumber(simulations: Seq[SimulationWithCores], maxDrop: Double): SimulationWithCores = {

    val sortedData = simulations.sortBy(_.executorNum)
    val dropRatios = sortedData
      .sliding(2)
      .collect { case Seq(prev, current) =>
        val dropPercentage = ((prev.appRuntimeEstimate.estimatedAppTimeMs - current.appRuntimeEstimate.estimatedAppTimeMs).toDouble * 100) /
          current.appRuntimeEstimate.estimatedAppTimeMs
        dropPercentage
      }
      .toList

    val optimalIndex = dropRatios
      .zipWithIndex
      .collect { case (drop, index) if drop >= maxDrop => index + 1 }
      .lastOption
      .getOrElse(0)

    sortedData(optimalIndex)
  }

  /**
   * Determines the optimal storage per executor based on shuffle write patterns.
   *
   * Uses uniform or non-uniform logic depending on observed shuffle write behavior across executors.
   *
   * @param executorCount The number of concurrent executors expected to run.
   * @return The recommended local storage per executor in bytes.
   */
  def recommendExecutorStorage(executorCount: Int): Long = {
    if (appContext.appSparkExecutors.isShuffleWriteUniform) {
      computeUniformStorage(executorCount)
    } else {
      computeNonUniformStorage()
    }
  }

  /**
   * Computes optimal executor storage assuming uniform shuffle write distribution.
   *
   * @param executorCount Number of expected concurrent executors.
   * @return Recommended local storage per executor in bytes.
   */
  private def computeUniformStorage(executorCount: Int): Long = {
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
   * @return Recommended local storage per executor in bytes.
   */
  private def computeNonUniformStorage(): Long = {
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
