package com.amazonaws.emr

import com.amazonaws.emr.report.ReportSparkSingleJob
import com.amazonaws.emr.spark.EmrSparkLogParser
import com.amazonaws.emr.utils.ArgParser
import com.amazonaws.emr.utils.Constants._

object SparkLogsAnalyzer extends App {

  val className = this.getClass.getCanonicalName.replace("$", "")
  val baseCmd = s"spark-submit --class $className"

  val argParser = new ArgParser(baseCmd, 1, SparkAppParams, SparkAppOptParams)
  val appParams = argParser.parse(args.toList)

  val sparkLogFile = appParams.getOrElse("filename", "")

  val logParser = new EmrSparkLogParser(sparkLogFile)
  val appContext = logParser.process()
  logParser.analyze(appContext, appParams)

  val userSettings = Set(ParamExecutors.name, ParamDuration.name)
  val hasCustomSettings = appParams.keySet.exists(userSettings.contains)

  val report = new ReportSparkSingleJob(appContext, hasCustomSettings)
  report.save(appParams)

}
