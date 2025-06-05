package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, SparkRuntime}
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, EnvironmentName, SERVERLESS}
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.apache.logging.log4j.scala.Logging

class SparkPerformanceOptimizer(
  sparkBaseOptimizer: SparkBaseOptimizer,
  sparkEnvOptimizer: SparkEnvOptimizer
) extends EnvOptimizer with Logging {

  override def recommend(
    simulations: Seq[SimulationWithCores],
    environment: EnvironmentName,
    optType: OptimalType,
    expectedTime: Option[Long] = None
  ): EmrEnvironment = {

    val initialSparkRuntime = SparkRuntime.fromAppContext(sparkBaseOptimizer.appContext)
    val sparkRuntimeBuilder = new SparkRuntimeBuilder(sparkBaseOptimizer)
    val maxDrop = Config.ExecutorsMaxDropLossPerformance

    val filteredSimulations = simulations
      .filter(_.appRuntimeEstimate.estimatedAppTimeMs <= initialSparkRuntime.runtime) match {
        case Seq() => simulations
        case filtered => filtered
      }

    val optimalSimulations = filteredSimulations
      .groupBy(_.coresPerExecutor)
      .map { case (_, group) => sparkBaseOptimizer.recommendExecutorNumber(group, maxDrop) }

    val runtimes = environment match {
      case EC2 | EKS =>
        optimalSimulations
          .map(sparkRuntimeBuilder.buildEc2)

      case SERVERLESS =>
        optimalSimulations
          .flatMap(sparkRuntimeBuilder.buildServerless(_))
    }

    val filteredRuntimes = runtimes.filter(runtime => runtime.executorMemory < byteStringAsBytes("128g"))

    val optimalRuntime = if (filteredRuntimes.nonEmpty) {
      filteredRuntimes.toList.sortWith { (a, b) =>
        if (a.executorCores != b.executorCores)
          a.executorCores > b.executorCores
        else if (a.executorsNum != b.executorsNum)
          a.executorsNum > b.executorsNum
        else
          a.runtime < b.runtime
      }.head
    } else {
      logger.warn("Filtered runtimes was empty!!")
      runtimes.minBy(_.runtime)
    }

    val environments = sparkEnvOptimizer.recommend(optimalRuntime, environment, optType, simulations)
    environments.minBy(_.costs.total)
  }

  private def computeScores(environments: Seq[EmrEnvironment]): Seq[(EmrEnvironment, Double)] = {
    def score(environment: EmrEnvironment): Double = {
      //println(s"${env.instances} - ${env.resources.cpuWastePercent}")
      val cpuWasteTarget = 0.2
      val wasteScore = 1.0 - math.abs(environment.resources.cpuWastePercent * 0.01 - cpuWasteTarget)

      val runtimeScore = 1.0 / (1.0 + environment.sparkRuntime.runtime.toDouble)
      val costScore = 1.0 / (1.0 + environment.costs.total)

      // Higher score is better: prioritize wasteScore, then runtime, then cost
      wasteScore * 1e6 + runtimeScore * 1e3 + costScore
    }

    environments.map(e => (e, score(e)))
  }

}