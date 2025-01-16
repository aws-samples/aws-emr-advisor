package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.api.AwsCosts.{EmrCosts, EmrOnEc2Cost, EmrOnEksCost, EmrServerlessCost}
import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, DefaultCurrency, EmrInstance, VolumeType}
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.utils.Formatter.printDuration

trait EmrEnvironment extends HtmlBase {

  val costs: EmrCosts

  val sparkRuntime: SparkRuntime

  val driver: ResourceRequest

  val executors: ResourceRequest

  def instances: List[String] = Nil

  def label: String

  def description: String

  def serviceIcon: String

  def totalCores: Int

  def totalMemory: Long

  def totalStorage: Long

  // HTML
  def htmlDescription: String

  def htmlCard(extended: Boolean = false, selected: Boolean = false): String = {
    s"""
       |  <div class="card ${if (selected) "border-success" else ""}">
       |    <div class="card-header">
       |      <div class="float-start p-2">$serviceIcon</div>
       |      <h5 class="card-title pt-1">$label</h5>
       |      <h6 class="card-subtitle text-muted float-start">$description</h6>
       |    </div>
       |    <div class="card-body position-relative">
       |      ${if (extended) """<p class="card-text mb-4">""" + htmlDescription + "</p>" else ""}
       |      <p class="card-text mb-2"><b>Cluster Resources</b></p>
       |      $htmlHardware
       |      <p class="card-text mb-2 mt-4"><b>Cluster Topology</b></p>
       |      $htmlResources
       |      <p class="card-text mb-2 mt-4"><b>Cost Estimation</b></p>
       |      $htmlCosts
       |      <p class="card-text mb-2 mt-4"><b>Notes</b></p>
       |      $htmlNotes
       |    </div>
       |    <div class="card-footer text-muted">
       |      <div class="float-start"><b>Total Costs</b></div>
       |      <div class="float-end">${"%.2f".format(costs.total)} $DefaultCurrency</div>
       |    </div>
       |  </div>
       |""".stripMargin
  }

  def htmlCosts: String = htmlGroupListWithFloat(
    Seq(
      (s"""EMR Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.emr)} $DefaultCurrency"),
      (s"""EC2 Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.hardware)} $DefaultCurrency"),
      (s"""EBS Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.storage)} $DefaultCurrency"),
      (s"""<B>Total Costs</B> ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.emr + costs.hardware + costs.storage)} $DefaultCurrency")
    ), "list-group-flush mb-2", "px-0"
  )

  def htmlExample(appInfo: AppInfo): String

  def htmlHardware: String = htmlHardwareResources(totalCores, totalMemory, totalStorage)

  def htmlNotes: String = htmlGroupList(htmlFinalNotes, "list-group-flush fs-6 mb-2", "px-0")

  def htmlResources: String

  def htmlServiceNotes: Seq[String]

  def htmlFinalNotes: Seq[String] = if (costs.spotDiscount > 0) {
    htmlTextSmall(s"""* EC2 costs computed for <b>SPOT</b> instances using a ${costs.spotDiscount * 100}% discount""") +: htmlServiceNotes
  } else htmlServiceNotes

  def toDebugStr: String = {
    s"""
       |Runtime: ${printDuration(sparkRuntime.runtime)}
       |Costs  : $$ ${costs.total}
       |${sparkRuntime.toString}
       |""".stripMargin
  }

}

object Environment extends Enumeration {

  type EnvironmentName = Value
  val EC2, EKS, SERVERLESS = Value

  def emptyEmrOnEc2: EmrOnEc2Env = EmrOnEc2Env(
    emptyInstance,
    emptyInstance,
    coreNodeNum = 0,
    SparkRuntime.empty,
    yarnContainersPerInstance = 0,
    EmrOnEc2Cost(0, 0, 0, 0),
    YarnRequest(0, 0, 0, 0),
    YarnRequest(0, 0, 0, 0)
  )

  def emptyEmrOnEks: EmrOnEksEnv = EmrOnEksEnv(
    driverInstance = emptyInstance,
    executorInstance = emptyInstance,
    executorInstanceNum = 0,
    SparkRuntime.empty,
    podsPerInstance = 0,
    EmrOnEksCost(0, 0, 0, 0),
    K8sRequest(0, 0, 0, 0),
    K8sRequest(0, 0, 0, 0)
  )

  def emptyEmrServerless: EmrServerlessEnv = EmrServerlessEnv(
    totalCores = 0,
    totalMemory = 0,
    totalStorage = 0,
    architecture = ArchitectureType.X86_64,
    driver = K8sRequest(0, 0, 0, 0),
    executors = K8sRequest(0, 0, 0, 0),
    SparkRuntime.empty,
    EmrServerlessCost(0, 0, 0, 0),
    task_cpus = 1
  )

  private def emptyInstance: EmrInstance = EmrInstance(
    instanceType = "",
    instanceFamily = "",
    currentGeneration = false,
    vCpu = 0,
    memoryGiB = 0,
    volumeType = VolumeType.EBS,
    volumeNumber = 0,
    volumeSizeGB = 0,
    networkBandwidthGbps = 0,
    yarnMaxMemoryMB = 0,
    ec2Price = 0,
    emrPrice = 0
  )
}