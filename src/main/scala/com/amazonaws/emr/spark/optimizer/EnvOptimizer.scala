package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.EmrEnvironment
import com.amazonaws.emr.spark.models.runtime.Environment.EnvironmentName

trait EnvOptimizer {

  def recommend(
    simulations: Seq[SimulationWithCores],
    environment: EnvironmentName,
    optType: OptimalType,
    expectedTime: Option[Long] = None
  ): EmrEnvironment

}
