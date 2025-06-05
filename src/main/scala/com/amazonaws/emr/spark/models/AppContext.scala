package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}

/**
 * AppContext is the central in-memory representation of a parsed Spark application execution.
 *
 * It encapsulates all the runtime, configuration, metric, and timeline data needed for
 * downstream analysis, optimization, and reporting.
 *
 * The object is typically constructed after parsing has been completed,
 * and it is used by analyzers to:
 *   - Evaluate efficiency metrics
 *   - Simulate alternate runtime configurations
 *   - Generate Spark runtime recommendations
 *   - Produce user-facing insights and cost estimations
 *
 * @param appInfo            High-level metadata about the Spark application (e.g., name, duration, runtime mode).
 * @param appConfigs         Parsed configuration parameters from Spark, Hadoop, JVM, and system properties.
 * @param appMetrics         Aggregated and raw task/job/stage-level Spark metrics.
 * @param hostMap            Mapping of hostnames to time spans (for diagnostics and cluster layout analysis).
 * @param executorMap        Mapping of executor IDs to their respective time spans and task metrics.
 * @param jobMap             Mapping of job IDs to job-level execution windows and metrics.
 * @param jobSQLExecIdMap    Mapping of job IDs to associated SQL execution IDs (if applicable).
 * @param stageMap           Mapping of stage IDs to time spans and DAG relationships.
 * @param stageIDToJobID     Reverse lookup mapping from stage ID to parent job ID.
 */
class AppContext(
  val appInfo: AppInfo,
  val appConfigs: AppConfigs,
  val appMetrics: AppMetrics,
  val hostMap: Map[String, HostTimeSpan],
  val executorMap: Map[String, ExecutorTimeSpan],
  val jobMap: Map[Long, JobTimeSpan],
  val jobSQLExecIdMap: Map[Long, Long],
  val stageMap: Map[Int, StageTimeSpan],
  val stageIDToJobID: Map[Int, Long]
) {

  /** Analyzed efficiency metrics such as executor utilization, GC overhead, and task skew. */
  val appEfficiency = new AppEfficiency

  /** Environment-specific Spark and EMR configuration recommendations. */
  val appRecommendations = new AppRecommendations

  /** Aggregated information and utilities derived from the executorMap and config context. */
  val appSparkExecutors = new AppSparkExecutors(executorMap, appConfigs)

}
