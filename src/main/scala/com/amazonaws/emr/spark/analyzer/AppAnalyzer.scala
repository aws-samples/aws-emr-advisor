package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.spark.models.AppContext
import org.apache.logging.log4j.scala.Logging

/**
 * The `AppAnalyzer` trait defines the contract for implementing Spark application analysis components.
 *
 * It supports both high-level and time-bounded analysis of an application, providing flexibility for analyzers
 * that evaluate runtime metrics, efficiency, optimization, or structural insights from the application's
 * event timeline.
 */
trait AppAnalyzer {

  /**
   * Primary method to trigger application analysis using known start and end times.
   *
   * @param appContext Application metadata and metrics context.
   * @param startTime  Start timestamp (epoch ms).
   * @param endTime    End timestamp (epoch ms).
   * @param options    Analysis parameters (e.g., tuning flags, region, duration).
   */
  def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit

  /**
   * Convenience method to run analysis using the application's built-in start and end times.
   *
   * @param ac      Application context.
   * @param options Optional analysis parameters.
   */
  def analyze(ac: AppContext, options: Map[String, String]): Unit =
    analyze(ac, ac.appInfo.startTime, ac.appInfo.endTime, options)

}

/**
 * The `AppAnalyzer` companion object provides a utility to launch a predefined
 * sequence of analysis modules over a Spark application's context.
 */
object AppAnalyzer extends Logging {

  /**
   * Starts the full analysis pipeline for the given application using default parameters.
   *
   * @param appContext The context containing all metrics and metadata.
   */
  def start(appContext: AppContext): Unit = start(appContext, Map.empty[String, String])

  /**
   * Starts the full analysis pipeline with custom options.
   *
   * @param appContext The context containing metrics, configs, and timelines.
   * @param options    Optional analysis parameters (e.g., region, max executors).
   */
  def start(appContext: AppContext, options: Map[String, String]): Unit = {

    val analyzers = List(
      new AppRuntimeAnalyzer,
      new AppEfficiencyAnalyzer,
      new AppOptimizerAnalyzer,
      new AppInsightsAnalyzer
    )

    analyzers.foreach(x => {
      try {
        x.analyze(appContext, options)
      } catch {
        case e: Throwable =>
          logger.error(s"Failed in Analyzer ${x.getClass.getSimpleName}")
          e.printStackTrace()
      }
    })
  }

}
