package com.amazonaws.emr.report

import com.amazonaws.emr.cluster.ClusterInfo
import com.amazonaws.emr.report.cluster.{PageCosts, PageSummary}

class ReportCluster(clusterInfo: ClusterInfo) extends HtmlReport {

  override def title: String = clusterInfo.id

  override def reportName: String = "Cluster"

  override def reportPrefix: String = "cluster"

  override def pages: List[HtmlPage] = List(
    new PageSummary(clusterInfo),
    new PageCosts(clusterInfo)
  )

}
