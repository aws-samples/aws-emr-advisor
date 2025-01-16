package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.Environment.EnvironmentName
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, SparkRuntime}

import scala.collection.mutable

object OptimalTypes extends Enumeration {
  type OptimalType = Value
  val TimeOpt, CostOpt, UserDefinedOpt: OptimalType = Value
}

class AppRecommendations {

  var currentSparkConf: Option[SparkRuntime] = None
  val recommendations: mutable.Map[EnvironmentName, mutable.Map[OptimalType, EmrEnvironment]] =
    mutable.HashMap.empty
  var simulations: Option[Seq[SimulationWithCores]] = None

  def getRecommendations(env: EnvironmentName): mutable.Map[OptimalType, EmrEnvironment] =
    recommendations.getOrElse(env, mutable.HashMap.empty)
}