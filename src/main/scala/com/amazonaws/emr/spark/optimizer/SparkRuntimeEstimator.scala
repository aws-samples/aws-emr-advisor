package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.spark.analyzer.{AppRuntimeEstimate, SimulationWithCores}
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.scheduler.CompletionEstimator
import org.apache.logging.log4j.scala.Logging

object SparkRuntimeEstimator extends Logging {

  def estimate(
    appContext: AppContext,
    coresPerExecutor: Int,
    maxExecutors: Int
  ): Seq[SimulationWithCores] = {

    logger.debug(s"Estimate $coresPerExecutor - (1 to $maxExecutors)")
    val executorsTests = (1 to maxExecutors).par
    executorsTests.map { executorsCount =>
      val (estimatedAppTime, driverTime) = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
        appContext,
        executorsCount,
        coresPerExecutor,
        appContext.appInfo.duration
      )
      val estimatedTotalCoreMs = (estimatedAppTime - driverTime) * executorsCount * coresPerExecutor
      SimulationWithCores(coresPerExecutor, executorsCount, AppRuntimeEstimate(estimatedAppTime, estimatedTotalCoreMs))
    }.seq

  }

}
