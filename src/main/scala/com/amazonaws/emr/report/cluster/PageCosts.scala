package com.amazonaws.emr.report.cluster

import com.amazonaws.emr.api.{AwsEmr, AwsPricing}
import com.amazonaws.emr.cluster.ClusterInfo
import com.amazonaws.emr.report.HtmlPage
import software.amazon.awssdk.regions.Region

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.util.Try

case class EmrEc2Instance(instanceId: String, instanceType: String, billableTimeMs: Long)

case class EmrEc2InstanceCosts(instanceId: String, total: Double, emr: Double, ec2: Double, ebs: Double = 0)

class PageCosts(clusterInfo: ClusterInfo) extends HtmlPage {

  override def content: String = {
    s"""
       |<div class="row">
       |  <div class="col-sm">
       |    <h5>Cluster Costs</h5>
       |      $clusterCostsTable
       |  </div>
       |</div>
       |""".stripMargin
  }

  override def pageId: String = "costs"

  override def pageName: String = "costs"

  override def pageIcon: String = "currency-dollar"

  val awsRegion = clusterInfo.region
  // get instances with corresponding price
  val allInstancePrice = AwsPricing.getEmrAvailableInstances(awsRegion)

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  // get cluster instances
  val clusterInstances = AwsEmr
    .listInstances(clusterInfo.id, Region.of(awsRegion))
    .map { i =>
      val creationDateTime = i.status().timeline().creationDateTime().toString
      val terminationDateTime = Try {
        i.status().timeline().endDateTime().toString
      }.getOrElse(LocalDateTime.now().format(formatter))

      val creation = LocalDateTime.parse(creationDateTime, formatter).atZone(ZoneId.of("UTC")).toEpochSecond
      val termination = LocalDateTime.parse(terminationDateTime, formatter).atZone(ZoneId.of("UTC")).toEpochSecond
      val billableTimeMs = termination - creation

      EmrEc2Instance(
        i.ec2InstanceId(),
        i.instanceType(),
        billableTimeMs
      )
    }

  // compute costs
  val clusterInstancesCosts = clusterInstances.map { i =>
    val runtime = i.billableTimeMs
    val instancePrice = allInstancePrice.filter(e => e.instanceType.equalsIgnoreCase(i.instanceType)).head
    val runtimeHours = runtime / (3600 * 1000.0)
    val ec2Cost: Double = s"%.${6}f".format(instancePrice.ec2Price * runtimeHours).toDouble
    val emrCost: Double = s"%.${6}f".format(instancePrice.emrPrice * runtimeHours).toDouble
    val totalCosts = ec2Cost + emrCost
    EmrEc2InstanceCosts(i.instanceId, totalCosts, emrCost, ec2Cost)
  }

  val totalEc2Costs = clusterInstancesCosts.map(_.ec2).sum
  val totalEmrCosts = clusterInstancesCosts.map(_.emr).sum
  val totalClusterCosts = totalEc2Costs + totalEmrCosts

  val clusterCostsTable = htmlTable(
    Nil,
    List(
      List("EMR Costs", s"%.${2}f".format(totalEmrCosts)),
      List("EC2 Costs", s"%.${2}f".format(totalEc2Costs)),
      List("EBS Costs", "TODO"),
      List(htmlBold("Total Costs"), s"%.${2}f".format(totalClusterCosts)),
    ), CssTableStyle)

}
