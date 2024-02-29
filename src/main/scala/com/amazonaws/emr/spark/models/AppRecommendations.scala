package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, SparkRuntime}

import scala.collection.SortedMap

class AppRecommendations {

  var currentSparkConf: Option[SparkRuntime] = None
  var optimalSparkConf: Option[SparkRuntime] = None

  var emrOnEc2Environment: Option[EmrEnvironment] = None
  var emrOnEksEnvironment: Option[EmrEnvironment] = None
  var emrServerlessEnvironment: Option[EmrEnvironment] = None

  // contains simulations of application runtime using `optimalSparkConf`
  // contains number of executor and estimated runtime in ms
  var executorSimulations: Option[SortedMap[Int, Long]] = None

}