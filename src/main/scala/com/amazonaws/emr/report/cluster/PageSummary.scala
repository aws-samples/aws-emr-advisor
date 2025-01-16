package com.amazonaws.emr.report.cluster

import com.amazonaws.emr.cluster.ClusterInfo
import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.utils.Constants.{LinkConsoleEc2Cluster, LinkConsoleEc2Subnet}
import com.amazonaws.emr.utils.Formatter.{printDate, printDuration, printTime}

class PageSummary(clusterInfo: ClusterInfo) extends HtmlPage {

  private val startTime =
    s"""<span class="fw-bold text-black-50">
       |  ${printDate(clusterInfo.startTime)}
       |</span> ${printTime(clusterInfo.startTime)}""".stripMargin

  private val runtime = printDuration(clusterInfo.duration())

  override def pageId: String = "summary"

  override def pageName: String = "Summary"

  override def pageIcon: String = "house"

  override def isActive: Boolean = true

  override def content: String = {

    val htmlTabs = Seq(
      ("summaryCluster", "Cluster", htmlClusterSummary),
      ("summaryInsights", "Insights", htmlInsights)
    )

    htmlNavTabs(
      "summaryTab",
      htmlTabs,
      "summaryCluster",
      "nav-pills border navbar-light bg-light",
      "mt-3 text-break"
    )
  }

  val subnetConsoleLink = htmlLink(
    clusterInfo.subnetId,
    LinkConsoleEc2Subnet(clusterInfo.subnetId, clusterInfo.region)
  )
  val clusterConsoleLink = htmlLink(
    clusterInfo.id,
    LinkConsoleEc2Cluster(clusterInfo.id, clusterInfo.region)
  )

  private def accountTable: String = htmlTable(
    Nil,
    List(
      List("Account", clusterInfo.accountId),
      List("Region", clusterInfo.region),
    ), CssTableStyle)

  private def detailsTable: String = htmlTable(
    Nil,
    List(
      List("Id", clusterConsoleLink),
      List("Release", clusterInfo.emrRelease),
      List("Runtime", runtime),
      List("Launched", startTime)
    ), CssTableStyle)

  private def networkTable: String = htmlTable(
    Nil,
    List(
      List("Subnet", s"$subnetConsoleLink (${clusterInfo.availabilityZone})"),
      List("CIDR", clusterInfo.subnetCidr),
      List("Available Ip", clusterInfo.subnetAvailableIp.toString),
    ), CssTableStyle)

  private def topologyTable: String = htmlTable(
    Nil,
    List(
      List("Primary", s"(${clusterInfo.masterInstanceMarket}) ${clusterInfo.masterInstanceType}"),
      List("Core", s"(${clusterInfo.coreInstanceMarket}) ${clusterInfo.coreInstanceType}"),
      List("Task", s"(${clusterInfo.taskInstanceMarket}) ${clusterInfo.taskInstanceType}"),
    ), CssTableStyle)

  private def htmlClusterSummary: String = {
    s"""
       |<div class="row">
       |  <div class="col-sm">
       |    <h5>Details</h5>
       |    $detailsTable
       |    $accountTable
       |  </div>
       |  <div class="col-sm">
       |    <h5>Networking</h5>
       |    $networkTable
       |    <h5>Topology</h5>
       |    $topologyTable
       |  </div>
       |</div>
       |""".stripMargin
  }

  private def htmlInsights: String = {

    val insightData = List(
      clusterInfo.insightsInfo.toSeq.sortBy(_._1).map(_._2),
      clusterInfo.insightsIssue.toSeq.sortBy(_._1).map(_._2),
      clusterInfo.insightsWarn.toSeq.sortBy(_._1).map(_._2),
    ).flatten

    def clusterInsights: String = htmlGroupList(insightData, "list-group-flush", "px-2")

    s"""<div class="col-sm">
       |  $clusterInsights
       |</div>""".stripMargin
  }

}
