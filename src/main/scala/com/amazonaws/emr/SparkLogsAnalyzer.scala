package com.amazonaws.emr

import com.amazonaws.emr.report.ReportSparkSingleJob
import com.amazonaws.emr.spark.EmrSparkLogParser
import com.amazonaws.emr.utils.ArgParser
import com.amazonaws.emr.utils.Constants._

/**
 * Entry point for analyzing a single Spark application log file.
 *
 * The `SparkLogsAnalyzer` object parses command-line arguments, processes
 * a Spark event log using `EmrSparkLogParser`, and generates a performance
 * and cost analysis report using `ReportSparkSingleJob`.
 *
 * Supported arguments are defined in `SparkAppParams` and `SparkAppOptParams`,
 * and include inputs such as the event log filename, optimization targets
 * (e.g., duration, executor count), and output paths.
 *
 * Usage:
 * {{{
 *   spark-submit --class com.amazonaws.emr.SparkLogsAnalyzer --filename <event_log.json>
 * }}}
 */
object SparkLogsAnalyzer extends App {

  // Fully qualified class name (for constructing spark-submit command reference)
  val className = this.getClass.getCanonicalName.replace("$", "")
  val baseCmd = s"spark-submit --class $className"

  // Initialize argument parser for the Spark log analyzer
  val argParser = new ArgParser(baseCmd, 1, SparkAppParams, SparkAppOptParams)

  // Parse CLI arguments into a parameter map
  val appParams = argParser.parse(args.toList)

  // Extract the log file path from arguments
  val sparkLogFile = appParams.getOrElse("filename", "")

  // Parse and analyze the event log
  val logParser = new EmrSparkLogParser(sparkLogFile)
  val appContext = logParser.process()
  logParser.analyze(appContext, appParams)

  // Check for user-supplied tuning parameters
  val userSettings = Set(ParamExecutors.name, ParamDuration.name)
  val hasCustomSettings = appParams.keySet.exists(userSettings.contains)

  // Generate and save the final analysis report
  val report = new ReportSparkSingleJob(appContext, hasCustomSettings)
  report.save(appParams)

}
