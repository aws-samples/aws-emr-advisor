package com.amazonaws.emr.cluster

import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.{EmrExtraInstanceData, EmrInstanceControllerInfo}
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.utils.Constants._
import com.amazonaws.emr.utils.Formatter
import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import software.amazon.awssdk.regions.Region

import scala.io.Source
import collection.JavaConverters._

case class IgFleet(
  instanceGroupId: String,
  instanceGroupName: String,
  instanceRole: String,
  marketType: String,
  instanceType: String,
  requestedInstanceCount: Int
)

case class IcData(
  jobFlowId: String = "",
  jobFlowCreationInstant: Long = 0L,
  instanceCount: Int = 0,
  masterInstanceId: String = "",
  masterPrivateDnsName: String = "",
  masterInstanceType: String = "",
  slaveInstanceType: String = "",
  hadoopVersion: String = "",
  instanceGroups: List[IgFleet] = Nil,
)

class ClusterInfo extends Logging with HtmlBase {

  val insightsInfo = new scala.collection.mutable.HashMap[String, String]
  val insightsWarn = new scala.collection.mutable.HashMap[String, String]
  val insightsIssue = new scala.collection.mutable.HashMap[String, String]

  logger.info("Analyze cluster data...")

  // Collect Data
  private lazy val (icContent, eidContent) = try {
    (
      Source.fromFile(s"""/tmp/emr_advisor_cluster$EmrInstanceControllerInfo""").getLines.mkString,
      Source.fromFile(s"""/tmp/emr_advisor_cluster$EmrExtraInstanceData""").getLines.mkString
    )
  } catch {
    case e: Throwable =>
      e.printStackTrace()
      ("", "")
  }

  if (icContent.isEmpty) new RuntimeException("Unable to locate IC file")
  if (eidContent.isEmpty) new RuntimeException("Unable to locate Extra Instance Data file")

  implicit lazy val formats = DefaultFormats

  val icJson = parse(icContent)
  val icData = icJson.extract[IcData]
  val eidJson = parse(eidContent)

  // base info
  val id: String = icData.jobFlowId
  val startTime: Long = icData.jobFlowCreationInstant

  // instances info
  val masterInstanceId: String = icData.masterInstanceId

  val masterData = icData.instanceGroups.filter(_.instanceRole == "Master").head
  val coreData = icData.instanceGroups.filter(_.instanceRole == "Core").head
  val taskData = icData.instanceGroups.filter(_.instanceRole == "Task").head

  val masterInstanceType: String = masterData.instanceType
  val masterInstanceMarket: String = masterData.marketType
  val masterInstanceCount: Int = masterData.requestedInstanceCount

  val coreInstanceType: String = coreData.instanceType
  val coreInstanceMarket: String = coreData.marketType
  val coreInstanceCount: Int = coreData.requestedInstanceCount

  val taskInstanceType: String = taskData.instanceType
  val taskInstanceMarket: String = taskData.marketType
  val taskInstanceCount: Int = taskData.requestedInstanceCount

  // miscellaneous
  val region: String = (eidJson \\ "region").values.toString
  val accountId: String = (eidJson \\ "accountId").values.toString
  val emrRelease: String = (eidJson \\ "releaseLabel").values.toString
  val haCluster: Boolean = (eidJson \\ "haEnabled").values.toString.toBoolean

  val emrCluster = AwsEmr.describeEc2Cluster(id, Region.of(region))
  val subnetId = emrCluster.ec2InstanceAttributes().ec2SubnetId()
  val availabilityZone = emrCluster.ec2InstanceAttributes().ec2AvailabilityZone()

  val subnet = AwsEmr.describeSubnet(subnetId, Region.of(region)).head
  val subnetCidr = subnet.cidrBlock()
  val subnetAvailableIp = subnet.availableIpAddressCount()

  val tags = emrCluster.tags().asScala

  def clusterRuntime: Long = System.currentTimeMillis() - startTime

  def duration(): Long = System.currentTimeMillis() - startTime

  // Generate Cluster Insights
  checkRelease()
  checkLongRunning()
  checkAwsTags()

  /** Check if the cluster is using the latest emr version */
  private def checkRelease(): Unit = {
    val latest = AwsEmr.latestRelease(Region.of(region))
    if (latest.equalsIgnoreCase(emrRelease)) {
      insightsInfo("base_release") = htmlTextInfo(
        s"You're already using the latest EMR release ($emrRelease)")
    } else {
      insightsWarn("base_release") = htmlTextWarning(
        s"Your cluster is running on ${htmlBold(emrRelease)}. Please consider upgrading to the latest EMR release " +
          s"(${htmlBold(latest)}) for better performance.")
    }
  }

  private def checkLongRunning(): Unit = {
    val daysRunning = Formatter.toDays(clusterRuntime)
    if (daysRunning > Config.EmrLongRunningClusterDaysThreshold) {
      insightsInfo("base_resiliency_1") = htmlTextInfo(
        s"Your cluster is labelled as a ${htmlBold("Long Running")} cluster. ")

      if (!emrCluster.terminationProtected()) {
        insightsWarn("base_resiliency_2") = htmlTextWarning(
          s"""
             |This cluster has been running for $daysRunning days and doesn't have
             |EMR ${htmlLink(htmlBold("Termination Protection"), LinkEmrOnEc2TerminationProtection)} enabled.
             |Please consider enabling this feature for better resiliency.
             |""".stripMargin
        )
      }

      if (!haCluster) {
        insightsWarn("base_resiliency_3") = htmlTextWarning(
          s"""
             |This cluster has been running for $daysRunning days. Consider enabling the
             |EMR ${htmlLink(htmlBold("Multi Master"), LinkEmrOnEc2MultiMaster)} feature.
             |""".stripMargin
        )
      }

    } else {
      insightsInfo("base_resiliency_1") = htmlTextInfo(
        s"Your cluster is labelled as a ${htmlBold("Transient")} cluster.")
    }

  }

  private def checkAwsTags(): Unit = {
    if (emrCluster.hasTags) {
      insightsInfo("base_bp_1") = htmlTextInfo("Your cluster is using AWS Tags.")
    } else {
      insightsWarn("base_bp_1") = htmlTextWarning(
        s"""
           |The cluster does not have any AWS Tag associated.
           |Consider adding ${htmlLink(htmlBold("AWS Tags"), LinkEmrOnEc2AwsTags)} for auditing purposes.
           |""".stripMargin
      )
    }

  }

}
