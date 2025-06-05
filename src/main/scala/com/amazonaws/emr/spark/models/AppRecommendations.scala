package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.Environment.EnvironmentName
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, SparkRuntime}

import scala.collection.mutable

/**
 * Enumeration of optimization strategies for recommending Spark configurations.
 *
 * These types are used to guide the optimization objective when simulating runtime
 * environments and selecting the best deployment configuration.
 */
object OptimalTypes extends Enumeration {
  type OptimalType = Value
  val CostOpt, EfficiencyOpt, PerformanceOpt, UserDefinedOpt: OptimalType = Value
}

/**
 * AppRecommendations holds all optimized Spark runtime configurations
 * and environment-specific deployment suggestions for a given application.
 *
 * It captures:
 *   - The current runtime configuration (`currentSparkConf`) used by the app
 *   - Recommended configurations for various environments (EC2, EKS, Serverless)
 *     under different optimization strategies (e.g., cost, performance, efficiency)
 *
 * These values are populated by the `AppOptimizerAnalyzer` after simulation and scoring.
 *
 */
class AppRecommendations {

  /** The currently observed Spark configuration from the app context. */
  var currentSparkConf: Option[SparkRuntime] = None

  /**
   * A nested map of recommended configurations by environment and optimization type.
   *
   * Outer key: Environment (e.g., EC2, EKS, Serverless)
   * Inner key: Optimization type (Cost, Efficiency, etc.)
   * Value    : Recommended EMR environment configuration.
   */
  val recommendations: mutable.Map[EnvironmentName, mutable.Map[OptimalType, EmrEnvironment]] =
    mutable.HashMap.empty

  /**
   * Returns the map of optimization type to recommendation for a given environment.
   *
   * @param env Target environment name (e.g., EC2).
   * @return A map from `OptimalType` to recommended `EmrEnvironment`. Empty if not available.
   */
  def getRecommendations(env: EnvironmentName): mutable.Map[OptimalType, EmrEnvironment] =
    recommendations.getOrElse(env, mutable.HashMap.empty)
}