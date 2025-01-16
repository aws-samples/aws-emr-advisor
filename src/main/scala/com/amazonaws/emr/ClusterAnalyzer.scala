package com.amazonaws.emr

import com.amazonaws.emr.cluster.ClusterInfo
import com.amazonaws.emr.report.ReportCluster

object ClusterAnalyzer extends App {

  // check if the application is running on an EMR node
  val cluster = new ClusterInfo
  val report = new ReportCluster(cluster)

  report.save(Map())

}