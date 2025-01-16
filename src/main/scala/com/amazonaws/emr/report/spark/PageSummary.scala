package com.amazonaws.emr.report.spark

import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.models.{AppConfigs, AppInfo}
import com.amazonaws.emr.utils.Constants.{LinkSparkConf, LinkSparkQuickStart}
import com.amazonaws.emr.utils.Formatter.{humanReadableBytes, printDate, printDuration, printTime}

class PageSummary(appInfo: AppInfo, appConfigs: AppConfigs) extends HtmlPage {

  val applicationType = if (appInfo.isSparkStreaming) "Streaming Job" else "Batch Job"

  override def pageId: String = "summary"

  override def pageIcon: String = "house-fill"

  override def pageName: String = "Summary"

  override def isActive: Boolean = true

  override def content: String = {

    val htmlTabs = Seq(
      ("summaryApp", "Application", htmlAppSummary),
      ("summaryInsights", "Insights", htmlInsights)
    )

    val htmlFinalTabs = {
      if (appInfo.insightsTaskFailures.nonEmpty) {
        htmlTabs :+ ("summaryFailures", "Task Failures", htmlTaskFailures)
      } else htmlTabs
    }

    htmlNavTabs("summaryTab", htmlFinalTabs, "summaryApp", "nav-pills border navbar-light bg-light", "mt-3 text-break")
  }

  private def appInfoTable: String = htmlTable(
    Nil,
    List(
      List("Application ID", appInfo.applicationID),
      List("Application Name", appInfo.sparkAppName),
      List("Application Start", s"""${printTime(appInfo.startTime)} <span class="text-black-50 float-end">${printDate(appInfo.startTime)}</span>"""),
      List("Application End", s"""${printTime(appInfo.endTime)} <span class="text-black-50 float-end">${printDate(appInfo.endTime)}</span>"""),
      List("Application Runtime", printDuration(appInfo.duration))
    ), CssTableStyle)

  private def scalaRuntime: String = htmlTable(
    Nil,
    List(
      List("Application Language", "scala"),
      List("Application Class", appInfo.sparkCmd.get.appMainClass),
      List("Application Jar", appInfo.sparkCmd.get.appScriptJarPath),
      List("Application Params", appInfo.sparkCmd.get.appArguments.mkString(" ")),
      List("Application Type", applicationType),
    ), CssTableStyle)

  private def pyRuntime: String = htmlTable(
    Nil,
    List(
      List("Application Language", "python"),
      List("Application Script", appInfo.sparkCmd.get.appScriptJarPath),
      List("Application Params", appInfo.sparkCmd.get.appArguments.mkString(" ")),
      List("Application Type", applicationType),
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
        List("Spark Version", s"""${appConfigs.sparkVersion} / ${htmlLink("Documentation", LinkSparkQuickStart)} / ${htmlLink("Configs", LinkSparkConf)}"""),
        List("Scala Version", s"${appConfigs.javaConfigs.getOrElse("Scala Version", "NA").replace("version", "")}")
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

  private def htmlAppSummary: String = {
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
       |</div>
       |<div class="row">
       |  <div class="col-sm">
       |    $appConfigSparkTable
       |  </div>
       |  <div class="col-sm">
       |    $appConfigSparkExtTable
       |  </div>
       |</div>""".stripMargin
  }

  private def htmlInsights: String = {
    s"""<div class="col-sm">
       |  $appInfoInsights
       |</div>""".stripMargin
  }

  private def htmlTaskFailures: String = {
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

}
