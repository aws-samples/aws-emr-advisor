package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.timespan.StageSummaryMetrics

/**
 * AppEfficiency encapsulates efficiency-related metrics derived from a Spark application’s execution.
 *
 * These metrics are computed during post-processing and help categorize workload characteristics
 * such as compute utilization, memory pressure, scheduling bottlenecks, and GC overhead.
 *
 * === Categories ===
 * - **Application-level compute metrics**
 * - **Driver performance and delay indicators**
 * - **Executor resource usage and memory profiling**
 * - **Task-specific memory and result size patterns**
 *
 */
class AppEfficiency {

  // ------------------------------------------------------------------------
  // Application-level aggregate metrics
  // ------------------------------------------------------------------------

  /** Total wall-clock runtime of the application (ms). */
  var appTotalTime: Long = 0L

  /** Total number of core-milliseconds available from all executors across app lifetime. */
  var appTotalCoreAvailable: Long = 0L

  /** Total compute time available (cores × appTotalTime). */
  var appComputeMillisAvailable: Long = 0L

  /** Total compute time available while at least one job was active. */
  var inJobComputeMillisAvailable: Long = 0L

  /** Actual compute time used across all tasks during job windows. */
  var inJobComputeMillisUsed: Long = 0L

  /** Compute time wasted during job execution windows (idle executor time). */
  var inJobComputeMillisWasted: Long = 0L

  // ------------------------------------------------------------------------
  // Spark Driver metrics
  // ------------------------------------------------------------------------

  /** Total time the driver was not submitting jobs (ms). */
  var driverTime: Long = 0L

  /** Driver time as a percentage of the total application runtime. */
  var driverTimePercentage: Float = 0

  /** Percent of total available compute time wasted by the driver being inactive. */
  var driverWastedPercentOverAll: Float = 0

  /** Wasted core-milliseconds due to driver delay before job submission. */
  var driverComputeMillisWastedJobBased: Long = 0L

  /** Indicates large serialized task results returned to driver. */
  var isResultHeavy: Boolean = false

  /** Indicates long delays between job submissions (e.g., due to job orchestration). */
  var isSchedulingHeavy: Boolean = false

  // ------------------------------------------------------------------------
  // Executor resource utilization metrics
  // ------------------------------------------------------------------------

  /** Total percentage of wasted executor time over total app compute time. */
  var executorWastedPercentOverAll: Float = 0

  /** Total executor core time idle during job execution. */
  var executorUsed: Long = 0L

  /** Percent of available executor time actually used during jobs. */
  var executorUsedPercent: Float = 0

  /** Percent of executor compute wasted during jobs. */
  var executorWastedPercent: Float = 0

  /** Total time executors were actively processing jobs. */
  var executorsTime: Long = 0L

  /** Percentage of total app time spent by executors working on jobs. */
  var executorsTimePercentage: Float = 0

  /** Ideal lower-bound runtime assuming infinite executor capacity. */
  var executionTimeInfiniteResources: Long = 0L

  /** Ideal runtime assuming perfect parallelism on current resources. */
  var executionTimePerfectParallelism: Long = 0L

  /** Estimated runtime with a single executor and one core. */
  var executionTimeSingleExecutorOneCore: Long = 0L

  // ------------------------------------------------------------------------
  // Executor memory and storage metrics
  // ------------------------------------------------------------------------

  /** Total memory allocated across all executors. */
  var executorTotalMemoryBytes: Long = 0L

  /** Peak memory observed in any executor’s JVM heap. */
  var executorPeakMemoryBytes: Long = 0L

  /** Wasted memory (allocated - used). */
  var executorWastedMemoryBytes: Long = 0L

  /** Total memory spilled to disk across all executors. */
  var executorSpilledMemoryBytes: Long = 0L

  /** Maximum memory used by a task across all executors. */
  var executorsMaxTaskMemory: Long = 0L

  /** Aggregate task execution throughput (tasks/sec). */
  var executorsTasksPerSecond: Double = 0.0

  /** Executor core-to-memory ratio (for tuning GC/memory balance). */
  var executorCoreMemoryRatio: Long = 0L

  /** Indicates that tasks fully utilized CPU (often bottlenecked by compute). */
  var isCpuBound: Boolean = false

  /** Indicates high GC time as a percentage of task duration. */
  var isGcHeavy: Boolean = false

  /** Indicates tasks are short-lived (e.g., < 2 seconds on average). */
  var isTaskShort: Boolean = false

  /** Indicates high task-level memory usage or risk of OOMs. */
  var isTaskMemoryHeavy: Boolean = false

  // ------------------------------------------------------------------------
  // Stage-level performance metrics
  // ------------------------------------------------------------------------

  /** Stage summary metrics */
  var stageSummaryMetrics: List[StageSummaryMetrics] = Nil

  // ------------------------------------------------------------------------
  // Task-level performance metrics
  // ------------------------------------------------------------------------

  /** Max peak memory used by any task. */
  var taskMaxPeakMemory: Long = 0L

  /** Max memory spilled by any task. */
  var taskMaxMemorySpilled: Long = 0L

  /** Max disk I/O spill by any task. */
  var taskMaxDiskSpilled: Long = 0L

  /** Max serialized result size returned to the driver. */
  var taskMaxResultSize: Long = 0L

}
