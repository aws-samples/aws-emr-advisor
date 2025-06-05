package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.normalizeEmrServerlessRuntime
import com.amazonaws.emr.spark.models.runtime.SparkRuntime

/**
 * Builder class to generate optimized SparkRuntime configurations for various deployment environments.
 *
 * This builder relies on a SparkBaseOptimizer to recommend memory, core, and storage settings
 * based on historical application metrics and simulation results. It supports generation of configurations
 * for EC2-based clusters (YARN, K8s) and EMR Serverless.
 *
 * @param optimizer An instance of SparkBaseOptimizer used to generate resource recommendations.
 */
class SparkRuntimeBuilder(optimizer: SparkBaseOptimizer) {

  /**
   * Builds a SparkRuntime configuration using existing runtime settings and simulation runtime estimates
   * for EC2-based Spark clusters.
   *
   * @param sparkRuntime Current runtime configuration with initial memory/core settings.
   * @param simulationWithCores Simulation result with predicted application runtime.
   * @return A SparkRuntime instance incorporating storage recommendations and estimated runtime.
   */
  def buildEc2(sparkRuntime: SparkRuntime, simulationWithCores: SimulationWithCores): SparkRuntime = {
    val driverCores = sparkRuntime.driverCores
    val driverMemory = sparkRuntime.driverMemory
    val executorCores = sparkRuntime.executorCores
    val executorMemory = sparkRuntime.executorMemory
    val executorNumber = sparkRuntime.executorsNum
    val executorStorage = optimizer.recommendExecutorStorage(sparkRuntime.executorsNum)

    SparkRuntime(
      runtime = simulationWithCores.appRuntimeEstimate.estimatedAppTimeMs,
      driverCores = driverCores,
      driverMemory = driverMemory,
      executorCores = executorCores,
      executorMemory = executorMemory,
      executorStorageRequired = executorStorage,
      executorsNum = executorNumber
    )
  }

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
   * @param sim        Simulation result specifying executor core count and number of executors
   * @return A fully-formed SparkRuntime config with optimal memory, cores, and storage settings
   */
  def buildEc2(sim: SimulationWithCores): SparkRuntime = {
    val executorMemory = optimizer.recommendExecutorMemory(sim.coresPerExecutor, sim.executorNum)
    val executorStorage = optimizer.recommendExecutorStorage(sim.executorNum)
    val driverCores = optimizer.recommendDriverCores(sim.executorNum)
    val driverMemory = optimizer.recommendDriverMemory()

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
   * Constructs a SparkRuntime configuration tailored to EMR Serverless constraints,
   * starting from simulation data.
   *
   * Ensures output adheres to valid EMR Serverless worker configurations and normalizes settings
   * to match supported core/memory tiers.
   *
   * @param sim Simulated runtime configuration with cores and executor count.
   * @return Some(SparkRuntime) with valid normalized settings if successful, None otherwise.
   */
  def buildServerless(sim: SimulationWithCores): Option[SparkRuntime] = {
    val driverCores = optimizer.recommendDriverCores(sim.executorNum)
    val driverMemory = optimizer.recommendDriverMemory()
    val executorMemory = optimizer.recommendExecutorMemory(sim.coresPerExecutor, sim.executorNum)
    val executorStorage = optimizer.recommendExecutorStorage(sim.executorNum)
    val sparkConfig = SparkRuntime(
      runtime = sim.appRuntimeEstimate.estimatedAppTimeMs,
      driverCores = driverCores,
      driverMemory = driverMemory,
      executorCores = sim.coresPerExecutor,
      executorMemory = executorMemory,
      executorStorageRequired = executorStorage,
      executorsNum = sim.executorNum
    )
    Some(normalizeEmrServerlessRuntime(sparkConfig))
  }

  /**
   * Builds a normalized EMR Serverless SparkRuntime using an existing configuration and simulation estimate.
   *
   * This variant reuses current memory/core settings but re-estimates storage and runtime duration,
   * then normalizes the configuration to match EMR Serverless requirements.
   *
   * @param sparkRuntime Current SparkRuntime settings.
   * @param sim Simulation estimate for application runtime.
   * @return Some(SparkRuntime) with valid normalized settings if successful, None otherwise.
   */
  def buildServerless(sparkRuntime: SparkRuntime, sim: SimulationWithCores): Option[SparkRuntime] = {
    val driverCores = sparkRuntime.driverCores
    val driverMemory = sparkRuntime.driverMemory
    val executorCores = sparkRuntime.executorCores
    val executorMemory = sparkRuntime.executorMemory
    val executorNumber = sparkRuntime.executorsNum
    val executorStorage = optimizer.recommendExecutorStorage(sparkRuntime.executorsNum)
    val sparkConfig = SparkRuntime(
      runtime = sim.appRuntimeEstimate.estimatedAppTimeMs,
      driverCores = driverCores,
      driverMemory = driverMemory,
      executorCores = executorCores,
      executorMemory = executorMemory,
      executorStorageRequired = executorStorage,
      executorsNum = executorNumber
    )
    Some(normalizeEmrServerlessRuntime(sparkConfig))
  }

}
