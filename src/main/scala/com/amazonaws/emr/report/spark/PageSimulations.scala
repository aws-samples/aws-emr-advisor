package com.amazonaws.emr.report.spark

import com.amazonaws.emr.Config
import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.report.HtmlReport.{htmlBold, htmlBoxNote, htmlExecutorSimulationGraph, htmlGroupList}
import com.amazonaws.emr.spark.models.AppRecommendations
import com.amazonaws.emr.spark.models.runtime.SparkRuntime
import com.amazonaws.emr.utils.Constants.SparkConfigurationWarning
import com.amazonaws.emr.utils.Formatter.printDurationStr

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class PageSimulations(appRecommendations: AppRecommendations) extends HtmlPage {

  private val sparkConf = appRecommendations.optimalSparkConf.getOrElse(SparkRuntime.empty)
  private val simulations = appRecommendations.executorSimulations.getOrElse(Map.empty[Int, Long])
  private val recommended = appRecommendations.optimalSparkConf.getOrElse(SparkRuntime.empty).executorsNum
  private val estimatedTime = simulations.getOrElse(recommended, 0L)
  private val data = simulations.take(recommended + 50)

  private val labels = data.keys.toList
  private val values = data.values.map(Duration(_, TimeUnit.MILLISECONDS).toSeconds.toInt).toList

  private val graph = htmlExecutorSimulationGraph("executor-simulation", recommended, labels, values)

  private val explain = htmlGroupList(
    List(
      s"The graph shows the expected application runtime using different values for Spark executors",
      s"Simulations are generated for the following executors counts: <b>1</b> to ${htmlBold(s"${simulations.size}")}",
      s"Each simulation used uniform executors with ${htmlBold(s"${sparkConf.executorCores}")} cores",
      s"The recommended number of executors for this job is <b>$recommended</b>. Using this value, " +
        s"the expected runtime for the job is <b>${printDurationStr(estimatedTime)}</b>",
      s"This recommendation is computed evaluating the time distance between two consecutive executions. " +
        s"The value is found when the distance between two consecutive runs doesn't provide an additional time " +
        s"<b>reduction</b> of <b>${Config.ExecutorsMaxDropLoss} %</b>"
    ), "list-group-flush")

  override def render: String = {
    s"""
       |<h4>Application Runtime</h4>
       |$graph
       |$explain
       |
       |<div class="mt-4 row">
       |  <div class="col-sm">
       |    ${htmlBoxNote(SparkConfigurationWarning)}
       |  </div>
       |</div>
       |""".stripMargin
  }

}
