package com.amazonaws.emr.report.spark

import com.amazonaws.emr.utils.Constants.{LinkSparkConf, LinkSparkQuickStart}
import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.models.{AppConfigs, AppInfo}
import com.amazonaws.emr.utils.Formatter.{humanReadableBytes, printDate, printDuration, printTime}
import com.amazonaws.emr.report.HtmlReport.{htmlCodeBlock, htmlGroupList, htmlNavTabs, htmlTable}

class PageSummary(appInfo: AppInfo, appConfigs: AppConfigs) extends HtmlPage {

  private def appInfoTable: String = htmlTable(
    Nil,
    List(
      List("Application ID", appInfo.applicationID),
      List("Application Name", appInfo.sparkAppName),
      List("Application Start", s"""<span class="fw-bold text-black-50">${printDate(appInfo.startTime)}</span> ${printTime(appInfo.startTime)}"""),
      List("Application End", s"""<span class="fw-bold text-black-50">${printDate(appInfo.endTime)}</span> ${printTime(appInfo.endTime)}"""),
      List("Application Runtime", printDuration(appInfo.duration))
    ), CssTableStyle)

  private def scalaRuntime: String = htmlTable(
    Nil,
    List(
      List("Application Language", "scala"),
      List("Application Class", appInfo.sparkCmd.get.appMainClass),
      List("Application Jar", appInfo.sparkCmd.get.appScriptJarPath),
      List("Application Params", appInfo.sparkCmd.get.appArguments.mkString(" "))
    ), CssTableStyle)

  private def pyRuntime: String = htmlTable(
    Nil,
    List(
      List("Application Language", "python"),
      List("Application Script", appInfo.sparkCmd.get.appScriptJarPath),
      List("Application Params", appInfo.sparkCmd.get.appArguments.mkString(" "))
    ), CssTableStyle)

  private val insightData = List(
    appInfo.insightsInfo.toSeq.sortBy(_._1).map(_._2),
    appInfo.insightsIssue.toSeq.sortBy(_._1).map(_._2),
    appInfo.insightsWarn.toSeq.sortBy(_._1).map(_._2),
  ).flatten

  private def appInfoInsights: String = htmlGroupList(insightData, "list-group-flush", "px-2")

  private def appConfigCoreTable: String = {
    htmlTable(
      Nil,
      List(
        List("EMR Deployment", appInfo.runtime.deploymentInfo),
        List("EMR Release", appInfo.runtime.releaseInfo()),
        List("Java Version", s"${appConfigs.javaConfigs.getOrElse("Java Version", "NA")}"),
        List("Spark Version", s"""${appConfigs.sparkVersion} / <a href="$LinkSparkQuickStart" target="_blank">Doc</a> / <a href="$LinkSparkConf" target="_blank">Config</a>"""),
        List("Scala Version", s"${appConfigs.javaConfigs.getOrElse("Scala Version", "NA")}")
      ), CssTableStyle)
  }

  private def appConfigSparkTable: String = {
    htmlTable(
      Nil,
      List(
        List("Driver Cores", appConfigs.driverCores.toString),
        List("Driver Memory", humanReadableBytes(appConfigs.driverMemory)),
        List("Executor Cores", appConfigs.executorCores.toString),
        List("Executor Memory", humanReadableBytes(appConfigs.executorMemory))
      ), CssTableStyle)
  }

  private def appConfigSparkExtTable: String = {
    htmlTable(
      Nil,
      List(
        List("Spark Scheduler", appConfigs.sparkConfigs.getOrElse("spark.scheduler.mode", "FIFO")),
        List("Spark DRA Enabled", appConfigs.sparkConfigs.getOrElse("spark.dynamicAllocation.enabled", "false")),
        List("Spark Shuffle Service Enabled", appConfigs.sparkConfigs.getOrElse("spark.shuffle.service.enabled", "false")),
      ), CssTableStyle)
  }

  def htmlAppSummary: String = {
    s"""<div class="row">
       |  <div class="col-sm">
       |    $appInfoTable
       |  </div>
       |  <div class="col-sm">
       |    $appConfigCoreTable
       |  </div>
       |</div>
       |<div class="row">
       |  <div class="col-sm">
       |    ${if (appInfo.sparkCmd.get.isScala) scalaRuntime else if (appInfo.sparkCmd.get.isPython) pyRuntime}
       |  </div>
       |</div>""".stripMargin
  }

  def htmlSparkSummary: String = {
    s"""<div class="row">
       |  <div class="col-sm">
       |    $appConfigSparkTable
       |  </div>
       |  <div class="col-sm">
       |    $appConfigSparkExtTable
       |  </div>
       |</div>""".stripMargin
  }

  def htmlInsights: String = {
    s"""<div class="col-sm">
       |  $appInfoInsights
       |</div>""".stripMargin
  }

  def htmlTaskFailures: String = {
    s"""<div class="row">
       |  <div class="col-sm">
       |    <div id="list-example" class="list-group list-group-horizontal" style="display: grid !important; grid-template-columns: repeat(25, 1fr);">
       |      ${appInfo.insightsTaskFailures.map { x => s"""<a class="list-group-item list-group-item-action" style="border-radius:0px" href="#list-item-${x._1}">${x._1}</a>""" }.mkString}
       |    </div>
       |    <div data-bs-spy="scroll" data-bs-target="#list-example" data-bs-offset="0" class="scrollspy-example" style="height:500px" tabindex="0">
       |      ${appInfo.insightsTaskFailures.map { x => s"""<h5 id="list-item-${x._1}" class="mt-4">Task ${x._1}</h5>${htmlCodeBlock(x._2, "code")}""" }.mkString}
       |    </div>
       |  </div>
       |</div>
       |""".stripMargin
  }

  override def render: String = {

    val htmlTabs = Seq(
      ("summaryApp", "Application", htmlAppSummary),
      ("summaryInsights", "Insights", htmlInsights),
      ("summarySpark", "Spark", htmlSparkSummary),
    )

    val htmlFinalTabs = {
      if (appInfo.insightsTaskFailures.nonEmpty) {
        htmlTabs :+ ("summaryFailures", "Task Failures", htmlTaskFailures)
      } else htmlTabs
    }

    htmlNavTabs("summaryTab", htmlFinalTabs, "summaryApp", "nav-pills border navbar-light bg-light", "mt-4 text-break")
  }

}
