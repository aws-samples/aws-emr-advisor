package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, DefaultCurrency, EmrInstance, VolumeType}
import com.amazonaws.emr.api.AwsCosts.{EmrCosts, EmrOnEc2Cost, EmrOnEksCost, EmrServerlessCost}
import com.amazonaws.emr.report.HtmlReport.{htmlGroupList, htmlGroupListWithFloat, htmlHardwareResourcesCard, htmlTextSmall}
import com.amazonaws.emr.spark.models.AppInfo

trait EmrEnvironment {

  val costs: EmrCosts

  val driver: ResourceRequest

  val executors: ResourceRequest

  def exampleCreateIamRoles: String = ""

  def exampleRequirements(appInfo: AppInfo): String = ""

  def exampleSubmitJob(appInfo: AppInfo, conf: SparkRuntime): String = ""

  def instances: List[String] = Nil

  def label: String

  def totalCores: Int

  def totalMemory: Long

  def totalStorage: Long

  def htmlDescription: String

  def htmlResources: String

  def htmlServiceNotes: Seq[String]

  def htmlFinalNotes: Seq[String] = if (costs.spotDiscount > 0) {
    htmlTextSmall(s"""* EC2 costs computed for <b>SPOT</b> instances using a ${costs.spotDiscount * 100}% discount""") +: htmlServiceNotes
  } else htmlServiceNotes

  def htmlCosts: String = htmlGroupListWithFloat(
    Seq(
      (s"""EMR Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.emr)} $DefaultCurrency"),
      (s"""EC2 Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.hardware)} $DefaultCurrency"),
      (s"""EBS Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.storage)} $DefaultCurrency")
    ), "list-group-flush mb-2", "px-0"
  )

  def htmlHardware: String = htmlHardwareResourcesCard(totalCores, totalMemory, totalStorage)

  def htmlNotes: String = htmlGroupList(htmlFinalNotes, "list-group-flush fs-6 mb-2", "px-0")

  def toHtml: String = {
    s"""<p class="card-text">$htmlDescription</p>
       |
       |<p class="card-text mb-2 mt-4"><b>Cluster Resources</b></p>
       |$htmlHardware
       |<p class="card-text mb-2 mt-4"><b>Cluster Topology</b></p>
       |$htmlResources
       |<p class="card-text mb-2 mt-4"><b>Cost Estimation</b></p>
       |$htmlCosts
       |<p class="card-text mb-2 mt-4"><b>Notes</b></p>
       |$htmlNotes
       |""".stripMargin
  }

}

object Environment extends Enumeration {

  type name = Value
  val
  EC2,
  EKS,
  SERVERLESS = Value

  def emptyEmrOnEc2: EmrOnEc2Env = EmrOnEc2Env(
    emptyInstance,
    emptyInstance,
    0, SparkRuntime.empty, 0, EmrOnEc2Cost(0, 0, 0, 0), YarnRequest(0, 0, 0, 0), YarnRequest(0, 0, 0, 0)
  )

  def emptyEmrOnEks: EmrOnEksEnv = EmrOnEksEnv(
    EmrInstance("", "", currentGeneration = false, 0, 0, VolumeType.EBS, 0, 0, 0, 0, 0, 0),
    EmrInstance("", "", currentGeneration = false, 0, 0, VolumeType.EBS, 0, 0, 0, 0, 0, 0),
    0, SparkRuntime.empty, 0, EmrOnEksCost(0, 0, 0, 0), K8sRequest(0, 0, 0, 0), K8sRequest(0, 0, 0, 0))

  def emptyEmrServerless: EmrServerlessEnv = EmrServerlessEnv(
    0, 0, 0, ArchitectureType.X86_64, K8sRequest(0, 0, 0, 0), K8sRequest(0, 0, 0, 0), EmrServerlessCost(0, 0, 0, 0)
  )

  def emptyInstance: EmrInstance = EmrInstance(
    "", "", currentGeneration = false, 0, 0, VolumeType.EBS, 0, 0, 0, 0, 0, 0
  )

}

