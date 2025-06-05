package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, EnvironmentName, SERVERLESS}
import com.amazonaws.emr.spark.models.runtime._
import com.amazonaws.emr.utils.Formatter.printDuration
import org.apache.logging.log4j.scala.Logging

/**
 * SparkCostOptimizer evaluates Spark executor configurations in different EMR environments
 * (EC2, EKS, Serverless) and selects the one that minimizes cost, runtime, or waste depending on the optimization goal.
 *
 * This optimizer:
 *   - Builds Spark runtime configurations using simulation data
 *   - Converts them into EMR environment models
 *   - Scores each configuration using a weighted composite of cost, runtime, and waste
 *
 * Used in the cost-aware optimization strategy (`CostOpt`).
 *
 * @param sparkBaseOptimizer Base optimizer with access to app context and executor tuning logic
 * @param sparkEnvOptimizer  Maps Spark configs to environment-specific EMR cluster descriptions
 */
class SparkCostOptimizer(
  sparkBaseOptimizer: SparkBaseOptimizer,
  sparkEnvOptimizer: SparkEnvOptimizer
) extends EnvOptimizer with Logging {

  private val appContext = sparkBaseOptimizer.appContext
  private val currentSparkRuntime = SparkRuntime.fromAppContext(appContext)
  private val appExecutorCores = currentSparkRuntime.executorCores
  private val appExecutorNumber = currentSparkRuntime.executorsNum

  val sparkRuntimeBuilder = new SparkRuntimeBuilder(sparkBaseOptimizer)

  /**
   * Recommend the best EMR environment based on the selected optimization strategy.
   *
   * @param simulations Sequence of simulation data (runtime per executor config)
   * @param environment Target EMR environment (EC2, EKS, Serverless)
   * @param optType Optimization goal (CostOpt, EfficiencyOpt, PerformanceOpt, etc.)
   * @return The best-scoring EMR environment
   */
  def recommend(
    simulations: Seq[SimulationWithCores],
    environment: EnvironmentName,
    optType: OptimalType,
    expectedTime: Option[Long] = None
  ): EmrEnvironment = {

    val runtimeFilteredSimulations = expectedTime
      .map(threshold => simulations.filter(_.appRuntimeEstimate.estimatedAppTimeMs <= threshold))
      .filter(_.nonEmpty)
      .getOrElse(simulations)

    val matchingSimulation = if(expectedTime.nonEmpty) {
      runtimeFilteredSimulations
        .find(sim => appExecutorCores == sim.coresPerExecutor)
        .get
    } else {
      simulations
        .find(sim => appExecutorCores == sim.coresPerExecutor && appExecutorNumber == sim.executorNum)
        .get
    }

    logger.debug(s"Real    : ${printDuration(appContext.appInfo.duration)}")
    logger.debug(s"Expected: ${printDuration(matchingSimulation.appRuntimeEstimate.estimatedAppTimeMs)}")

    val runtimeConfigs = environment match {
      case EC2 | EKS => sparkRuntimeBuilder.buildEc2(currentSparkRuntime, matchingSimulation)
      case SERVERLESS =>
        val envWithCurrent = sparkRuntimeBuilder.buildServerless(currentSparkRuntime, matchingSimulation)
        if(envWithCurrent.isEmpty) {
          logger.warn(s"Can't use current Spark Configs for EMR Serverless")
          sparkRuntimeBuilder.buildServerless(matchingSimulation).head
        } else envWithCurrent.head
      case _ => SparkRuntime.empty
    }

    val computedEnvs = sparkEnvOptimizer.recommend(runtimeConfigs, environment, optType, simulations)
    computedEnvs.minBy(_.costs.total)

  }

  /** Case class for scoring components used in normalization */
  case class EnvScoreComponents(cost: Double, runtime: Double, waste: Double)

  /**
   * Computes a normalized score (0-1) for each EMR environment using a weighted sum.
   *
   * @param envs Sequence of EMR environments to score
   * @return Tuple of (environment, score) where lower score is better
   */
  def computeScores(envs: Seq[EmrEnvironment]): Seq[(EmrEnvironment, Double)] = {
    val scores = envs.map { env =>
      val cost = env.costs.total
      val time = env.sparkRuntime.runtime.toDouble
      val waste = env.resources.averageWastePercent
      (env, EnvScoreComponents(cost, time, waste))
    }

    val costs = scores.map(_._2.cost)
    val runtimes = scores.map(_._2.runtime)
    val wastes = scores.map(_._2.waste)

    def normalize(value: Double, min: Double, max: Double): Double =
      if (max > min) (value - min) / (max - min) else 0.0

    val minCost = costs.min; val maxCost = costs.max
    val minTime = runtimes.min; val maxTime = runtimes.max
    val minWaste = wastes.min; val maxWaste = wastes.max

    println(s"minCost: $minCost maxCost: $maxCost")
    println(s"minTime: $minTime maxTime: $maxTime")
    println(s"minWaste: $minWaste maxWaste: $maxWaste")
    
    val weights = (0.5, 0.3, 0.2)

    scores.map { case (env, comp) =>
      val normCost = normalize(comp.cost, minCost, maxCost)
      val normTime = normalize(comp.runtime, minTime, maxTime)
      val normWaste = normalize(comp.waste, minWaste, maxWaste)

      val score = normCost * weights._1 + normTime * weights._2 + normWaste * weights._3
      (env, score)
    }
  }

}