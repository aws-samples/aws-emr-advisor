package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.Config
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.OptimalTypes._
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, SERVERLESS}
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, SparkRuntime}
import com.amazonaws.emr.spark.optimizer.{SparkBaseOptimizer, SparkCostOptimizer, SparkTimeOptimizer}
import com.amazonaws.emr.spark.scheduler.{CompletionEstimator, StageRuntimeComparator}
import com.amazonaws.emr.utils.Constants._
import com.amazonaws.emr.utils.Formatter._
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Try

case class AppRuntimeEstimate(
  estimatedAppTimeMs: Long,
  estimatedTotalExecCoreMs: Long) {

  override def toString: String =
    s"App time: ${printDuration(estimatedAppTimeMs)}, Executors time: ${printDuration(estimatedTotalExecCoreMs)}"

}

case class SimulationWithCores(
  coresPerExecutor: Int,
  executorNum: Int,
  appRuntimeEstimate: AppRuntimeEstimate) {

  override def toString: String = s"cores: $coresPerExecutor, num_executors: $executorNum, $appRuntimeEstimate"

}

object AppRuntimeEstimate {
  val empty: AppRuntimeEstimate = AppRuntimeEstimate(0L, 0L)
}

class AppOptimizerAnalyzer extends AppAnalyzer with Logging {

  override def analyze(
    appContext: AppContext,
    startTime: Long,
    endTime: Long,
    options: Map[String, String]): Unit = {

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

    val currentConf = SparkRuntime.fromAppContext(appContext)
    appContext.appRecommendations.currentSparkConf = Some(currentConf)

    // generate base requirement and simulations
    val coresList = SparkBaseOptimizer.findOptCoresPerExecutor(appContext)
    if (coresList.isEmpty) throw new RuntimeException("coresList is empty.")

    val simulationList = coresList.flatMap { coreNum =>
      estimateRuntime(appContext, coreNum, maxExecutors).map {
        case (numExecutors, estimate) => SimulationWithCores(coreNum, numExecutors, estimate)
      }
    }

    val results = StageRuntimeComparator.compareRealVsEstimatedStageTimes(appContext)
    StageRuntimeComparator.printDebug(results)

    appContext.appRecommendations.simulations = Some(simulationList)

    val sparkCostOptimizer = new SparkCostOptimizer(awsRegion, spotDiscount)

    Seq(EC2, EKS, SERVERLESS).foreach { env =>

      // create map to save environment configurations
      val tempMap = mutable.HashMap[OptimalType, EmrEnvironment]()

      Seq(TimeOpt, CostOpt, UserDefinedOpt).foreach {

        case TimeOpt =>
          logger.debug(s"Analyzing Optimizations for $env $TimeOpt")
          SparkTimeOptimizer
            .findOptTimeSparkConf(appContext, simulationList, env)
            .flatMap(sparkRuntimeConfig => sparkCostOptimizer.findOptCostEnv(sparkRuntimeConfig, env, TimeOpt))
            .foreach(tempMap.put(TimeOpt, _))

        case CostOpt =>
          logger.debug(s"Analyzing Optimizations for $env $CostOpt")
          sparkCostOptimizer
            .findOptCostSparkConf(appContext, simulationList, env, CostOpt, Some(currentConf.runtime))
            .flatMap(sparkRuntimeConfig => sparkCostOptimizer.findOptCostEnv(sparkRuntimeConfig, env, CostOpt))
            .foreach(tempMap.put(CostOpt, _))

        case UserDefinedOpt =>
          logger.debug(s"Analyzing Optimizations for $env $UserDefinedOpt")
          sparkCostOptimizer
            .findOptCostSparkConf(appContext, simulationList, env, UserDefinedOpt, expectedDuration)
            .flatMap(sparkRuntimeConfig => sparkCostOptimizer.findOptCostEnv(sparkRuntimeConfig, env, UserDefinedOpt))
            .foreach(tempMap.put(UserDefinedOpt, _))

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

  def estimateRuntime(
    appContext: AppContext,
    coresPerExecutor: Int,
    maxExecutors: Int
  ): Seq[(Int, AppRuntimeEstimate)] = {

    logger.debug(s"Estimate $coresPerExecutor - (1 to $maxExecutors)")
    val executorsTests = (1 to maxExecutors).par
    executorsTests.map { executorsCount =>
      val (estimatedAppTime, driverTime) = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
        appContext,
        executorsCount,
        coresPerExecutor,
        appContext.appInfo.duration
      )

      val estimatedTotalCoreMs = (estimatedAppTime - driverTime) * executorsCount * coresPerExecutor
      executorsCount -> AppRuntimeEstimate(estimatedAppTime, estimatedTotalCoreMs)
    }.seq

  }

}




