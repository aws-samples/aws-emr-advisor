package com.amazonaws.emr.spark.models

class AppEfficiency {

  // ========================================================================
  // Spark Executor metrics
  // ========================================================================
  var appTotalTime: Long = 0L
  var appTotalCoreAvailable: Long = 0L

  var driverTime: Long = 0L
  var driverTimePercentage: Float = 0

  var driverWastedPercentOverAll: Float = 0
  var executorWastedPercentOverAll: Float = 0

  var executorUsed: Long = 0
  var executorUsedPercent: Float = 0
  var executorWastedPercent: Float = 0

  var executorsTime: Long = 0L
  var executorsTimePercentage: Float = 0

  var appComputeMillisAvailable = 0L
  var driverComputeMillisWastedJobBased = 0L

  var inJobComputeMillisAvailable = 0L
  var inJobComputeMillisUsed = 0L
  var inJobComputeMillisWasted = 0L

  var executionTimeInfiniteResources: Long = 0L
  var executionTimePerfectParallelism: Long = 0L
  var executionTimeSingleExecutorOneCore: Long = 0L

  // Executor Metrics
  var executorTotalMemoryBytes = 0L
  var executorPeakMemoryBytes = 0L
  var executorWastedMemoryBytes = 0L
  var executorSpilledMemoryBytes = 0L

  var executorsMaxTaskMemory = 0L
  var executorsTasksPerSecond = 0.0
  var executorsTasksPerSecondSingle = 0.0
  var executorCoreMemoryRatio = 0L

}
