package com.amazonaws.emr.spark.models

class AppEfficiency {

  // ========================================================================
  // Application metrics
  // ========================================================================
  var appTotalTime: Long = 0L
  var appTotalCoreAvailable: Long = 0L
  var appComputeMillisAvailable = 0L

  var inJobComputeMillisAvailable = 0L
  var inJobComputeMillisUsed = 0L
  var inJobComputeMillisWasted = 0L

  // ========================================================================
  // Spark Driver metrics
  // ========================================================================
  var driverTime: Long = 0L
  var driverTimePercentage: Float = 0
  var driverWastedPercentOverAll: Float = 0
  var driverComputeMillisWastedJobBased = 0L

  var isResultHeavy = false
  var isSchedulingHeavy = false

  // ========================================================================
  // Spark Executor metrics
  // ========================================================================
  var executorWastedPercentOverAll: Float = 0

  var executorUsed: Long = 0
  var executorUsedPercent: Float = 0
  var executorWastedPercent: Float = 0

  var executorsTime: Long = 0L
  var executorsTimePercentage: Float = 0

  var executionTimeInfiniteResources: Long = 0L
  var executionTimePerfectParallelism: Long = 0L
  var executionTimeSingleExecutorOneCore: Long = 0L

  var executorTotalMemoryBytes = 0L
  var executorPeakMemoryBytes = 0L
  var executorWastedMemoryBytes = 0L
  var executorSpilledMemoryBytes = 0L

  var executorsMaxTaskMemory = 0L
  var executorsTasksPerSecond = 0.0
  var executorCoreMemoryRatio = 0L

  var isCpuBound = false
  var isGcHeavy = false
  var isTaskShort = false
  var isTaskMemoryHeavy = false

  // ========================================================================
  // Spark Task metrics
  // ========================================================================
  var taskMaxPeakMemory = 0L
  var taskMaxMemorySpilled = 0L
  var taskMaxDiskSpilled = 0L
  var taskMaxResultSize = 0L

}
