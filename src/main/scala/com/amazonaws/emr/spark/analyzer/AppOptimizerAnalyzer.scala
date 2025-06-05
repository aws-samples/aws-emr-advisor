package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.Config
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.OptimalTypes._
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, SERVERLESS}
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, SparkRuntime}
import com.amazonaws.emr.spark.optimizer._
import com.amazonaws.emr.spark.scheduler.StageRuntimeComparator
import com.amazonaws.emr.utils.Constants._
import com.amazonaws.emr.utils.Formatter._
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Represents the result of a Spark application runtime simulation.
 *
 * @param estimatedAppTimeMs       Estimated total application runtime in milliseconds.
 * @param estimatedTotalExecCoreMs Total estimated executor core-milliseconds consumed.
 */
case class AppRuntimeEstimate(estimatedAppTimeMs: Long, estimatedTotalExecCoreMs: Long) {
  override def toString: String =
    s"App time: ${printDuration(estimatedAppTimeMs)}, Executors time: ${printDuration(estimatedTotalExecCoreMs)}"
}

/** A constant representing an empty or missing runtime estimate. */
object AppRuntimeEstimate {
  val empty: AppRuntimeEstimate = AppRuntimeEstimate(0L, 0L)
}

/**
 * Simulation result for a specific combination of executor cores and count.
 *
 * @param coresPerExecutor   Number of cores per executor used in the simulation.
 * @param executorNum        Number of executors simulated.
 * @param appRuntimeEstimate Estimated runtime and executor utilization.
 */
case class SimulationWithCores(coresPerExecutor: Int, executorNum: Int, appRuntimeEstimate: AppRuntimeEstimate) {
  override def toString: String = s"cores: $coresPerExecutor, num_executors: $executorNum, $appRuntimeEstimate"
}

/**
 * AppOptimizerAnalyzer evaluates multiple Spark executor configurations and environments
 * to recommend cost-, efficiency-, or performance-optimized deployment plans.
 *
 * It:
 *   - Simulates execution across core/executor configurations
 *   - Estimates runtime and compute cost on EC2, EKS, and Serverless
 *   - Applies different optimization strategies (CostOpt, EfficiencyOpt, PerformanceOpt)
 *   - Stores recommendations into `appContext.appRecommendations`
 *
 */
class AppOptimizerAnalyzer extends AppAnalyzer with Logging {

  /**
   * Performs full optimization analysis based on available simulations and strategies.
   *
   * @param appContext Spark application context with metrics and execution state.
   * @param startTime  App start timestamp (epoch ms).
   * @param endTime    App end timestamp (epoch ms).
   * @param options    Optimization parameters (e.g., max executors, region, discount).
   */
  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    // parameters
    val maxExecutors: Int = Try(
      options(normalizeName(ParamExecutors.name)).toInt
    ).getOrElse(Config.ExecutorsMaxTestsCount)

    val expectedDuration: Option[Long] = Try(
      Duration(options(ParamDuration.name)).toMillis
    ).toOption

    // Check for any ec2 spot discount to apply
    val spotDiscount: Double = options.getOrElse(ParamSpot.name, "0.0").toDouble

    // check for specific regions to compute costs
    val awsRegion = options.getOrElse(ParamRegion.name, DefaultRegion)

    logger.debug(s"Param maxExecutors: $maxExecutors")
    logger.debug(s"Param expectedDuration: $expectedDuration")
    logger.debug(s"Param spotDiscount: $spotDiscount")
    logger.debug(s"Param awsRegion: $awsRegion")

    val currentSparkRuntime = SparkRuntime.fromAppContext(appContext)
    appContext.appRecommendations.currentSparkConf = Some(currentSparkRuntime)

    // generate base requirement and simulations
    val sparkBaseOptimizer = new SparkBaseOptimizer(appContext)
    val sparkEnvOptimizer = new SparkEnvOptimizer(appContext, awsRegion, spotDiscount)
    val sparkCostOptimizer = new SparkCostOptimizer(sparkBaseOptimizer, sparkEnvOptimizer)
    val sparkEfficiencyOptimizer = new SparkEfficiencyOptimizer(sparkBaseOptimizer, sparkEnvOptimizer)
    val sparkPerformanceOptimizer = new SparkPerformanceOptimizer(sparkBaseOptimizer, sparkEnvOptimizer)

    logger.info(s"Generate Spark simulations with different sets of cores and max executors")
    val appExecutorNumber = currentSparkRuntime.executorsNum
    val appExecutorCores = currentSparkRuntime.executorCores
    val appMaxTestExecutors = if (appExecutorNumber < maxExecutors) maxExecutors else appExecutorNumber + 50
    val coresList = (sparkBaseOptimizer.recommendExecutorCores() :+ appExecutorCores).distinct
    if (coresList.isEmpty) throw new RuntimeException("coresList is empty.")
    val simulations = coresList.par.flatMap { coreNum =>
      SparkRuntimeEstimator.estimate(appContext, coreNum, appMaxTestExecutors)
    }.toList

    logger.info(s"Generate application recommendations")
    val results = StageRuntimeComparator.compareRealVsEstimatedStageTimes(appContext)
    StageRuntimeComparator.printDebug(results)

    Seq(EC2, EKS, SERVERLESS).foreach { env =>

      // create map to save environment configurations
      val tempMap = mutable.HashMap[OptimalType, EmrEnvironment]()

      Seq(CostOpt, EfficiencyOpt, PerformanceOpt, UserDefinedOpt).foreach {

        case CostOpt =>
          logger.info(s"Analyzing Optimizations for $env $CostOpt")
          val optCostEnv = sparkCostOptimizer.recommend(simulations, env, CostOpt)
          tempMap.put(CostOpt, optCostEnv)

        case EfficiencyOpt =>
          logger.info(s"Analyzing Optimizations for $env $EfficiencyOpt")
          val optEfficiencyEnv = sparkEfficiencyOptimizer.recommend(simulations, env, EfficiencyOpt)
          tempMap.put(EfficiencyOpt, optEfficiencyEnv)

        case PerformanceOpt =>
          logger.info(s"Analyzing Optimizations for $env $PerformanceOpt")
          val optPerformanceEnv = sparkPerformanceOptimizer.recommend(simulations, env, PerformanceOpt)
          tempMap.put(PerformanceOpt, optPerformanceEnv)

        case UserDefinedOpt =>
          if(expectedDuration.nonEmpty) {
            logger.info(s"Analyzing Optimizations for $env $UserDefinedOpt")
            val optCostEnv = sparkCostOptimizer.recommend(simulations, env, UserDefinedOpt, expectedDuration)
            tempMap.put(UserDefinedOpt, optCostEnv)
          }

        case _ =>
          logger.warn(s"Unknown optimization type for environment: $env")

      }

      appContext.appRecommendations.recommendations.put(env, tempMap)
      appContext.appRecommendations.recommendations.get(env).foreach(conf =>
        conf.foreach { pair =>
          logger.debug(s"Recommendations for $env ${pair._1} ${pair._2.toDebugStr} ")
        }
      )
    }

  }

}




