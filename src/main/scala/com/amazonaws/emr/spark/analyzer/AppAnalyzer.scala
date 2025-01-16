package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.spark.models.AppContext
import org.apache.logging.log4j.scala.Logging

trait AppAnalyzer {

  def analyze(ac: AppContext, options: Map[String, String]): Unit =
    analyze(ac, ac.appInfo.startTime, ac.appInfo.endTime, options)

  def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit

}

object AppAnalyzer extends Logging {

  def start(appContext: AppContext): Unit = start(appContext, Map.empty[String, String])

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
