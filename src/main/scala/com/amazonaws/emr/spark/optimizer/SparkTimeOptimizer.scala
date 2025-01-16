package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.runtime.Environment._
import com.amazonaws.emr.spark.models.runtime.{EmrServerlessEnv, SparkRuntime}
import org.apache.logging.log4j.scala.Logging

object SparkTimeOptimizer extends Logging {

  def findOptTimeSparkConf(
    appContext: AppContext,
    simulationList: Seq[SimulationWithCores],
    environment: EnvironmentName
  ): Option[SparkRuntime] = {

    val maxDrop = Config.ExecutorsMaxDropLoss
    val optimalSimulation = simulationList
      .groupBy(_.coresPerExecutor)
      .map { case (_, group) => findOptTimeNumExecutors(group, maxDrop) }
      .minBy(_.appRuntimeEstimate.estimatedAppTimeMs)

    Some(environment match {
      case EC2 | EKS =>
        SparkBaseOptimizer.createSparkRuntime(appContext, optimalSimulation)
      case SERVERLESS =>
        EmrServerlessEnv.normalizeSparkConfigs(
          SparkBaseOptimizer.createSparkRuntime(appContext, optimalSimulation)
        )
    })
  }

  private def findOptTimeNumExecutors(
    data: Seq[SimulationWithCores],
    maxDrop: Double
  ): SimulationWithCores = {

    val sortedData = data.sortBy(_.executorNum)
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

}
