package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.models.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}

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

  val appEfficiency = new AppEfficiency
  val appRecommendations = new AppRecommendations
  val appSparkExecutors = new AppSparkExecutors(executorMap, appConfigs)

}

