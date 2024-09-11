package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.analyzer.AppRuntimeEstimate
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.{SparkRuntime, EmrEnvironment}

import scala.collection.SortedMap
import scala.collection.mutable

object OptimalTypes extends Enumeration {
  type OptimalType = Value

  val TimeOpt,
  CostOpt,
  TimeCapped,
  CostCapped= Value
}
class AppRecommendations {

  var currentSparkConf: Option[SparkRuntime] = None
  
  val sparkConfs= mutable.HashMap[OptimalType, SparkRuntime]()

  val emrOnEc2Envs= mutable.HashMap[OptimalType, EmrEnvironment]()
  val emrOnEksEnvs= mutable.HashMap[OptimalType, EmrEnvironment]()
  val emrServerlessEnvs= mutable.HashMap[OptimalType, EmrEnvironment]()

  // contains simulations of application runtime using `optimalSparkConf`
  // contains number of executor and estimated runtime in ms
  var executorSimulations: Option[SortedMap[Int, AppRuntimeEstimate]] = None

  val additionalInfo = mutable.HashMap[OptimalType, mutable.HashMap[String, String]]()

}