package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, EnvironmentName, SERVERLESS}
import com.amazonaws.emr.spark.models.runtime.EmrEnvironment
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.apache.logging.log4j.scala.Logging

class SparkEfficiencyOptimizer(
  sparkBaseOptimizer: SparkBaseOptimizer,
  sparkEnvOptimizer: SparkEnvOptimizer
) extends EnvOptimizer with Logging {

  def recommend(
    simulations: Seq[SimulationWithCores],
    environment: EnvironmentName,
    optType: OptimalType,
    expectedTime: Option[Long] = None
  ): EmrEnvironment = {

    val sparkRuntimeBuilder = new SparkRuntimeBuilder(sparkBaseOptimizer)
    val maxDrop = Config.ExecutorsMaxDropLossEfficiency
    val optimalSimulations = simulations
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

    val filteredRuntimes = runtimes.filter { runtime =>
      runtime.executorMemory < byteStringAsBytes("128g") && runtime.executorCores != 1
    }

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
      runtimes.minBy(_.runtime)
    }

    val envs = sparkEnvOptimizer.recommend(optimalRuntime, environment, optType, simulations)
    envs.minBy(_.costs.total)

  }

}
