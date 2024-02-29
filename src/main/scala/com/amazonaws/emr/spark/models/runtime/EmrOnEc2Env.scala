package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.{EmrOnEc2MinStorage, EmrOnEc2ProvisioningMs}
import com.amazonaws.emr.api.AwsCosts.EmrOnEc2Cost
import com.amazonaws.emr.api.AwsPricing.EmrInstance
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes, printDurationStr, toMB}
import com.amazonaws.emr.report.HtmlReport._
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.spark.models.runtime.EmrOnEc2Env.{getClusterClassifications, getNodeUsableMemory}

case class EmrOnEc2Env(
  masterNode: EmrInstance,
  coreNode: EmrInstance,
  coreNodeNum: Int,
  sparkRuntimeConfigs: SparkRuntime,
  yarnContainersPerInstance: Int,
  costs: EmrOnEc2Cost,
  driver: ResourceRequest,
  executors: ResourceRequest
) extends Ordered[EmrOnEc2Env] with EmrEnvironment {

  val baseStorage = byteStringAsBytes(EmrOnEc2MinStorage)
  val sparkStorage = yarnContainersPerInstance * executors.storage
  val workerStorage = baseStorage + sparkStorage

  def compare(that: EmrOnEc2Env): Int = this.costs.total compare that.costs.total

  override def instances: List[String] = List(masterNode.instanceType, coreNode.instanceType)

  override def label: String = "Emr On Ec2"

  override def totalCores: Int = coreNodeNum * coreNode.vCpu + masterNode.vCpu

  override def totalMemory: Long = coreNodeNum * byteStringAsBytes(s"${coreNode.memoryGiB}g") + byteStringAsBytes(s"${masterNode.memoryGiB}g")

  override def totalStorage: Long = coreNodeNum * workerStorage + baseStorage

  override def htmlDescription: String =
    s"""To orchestrate your Spark jobs on Hadoop efficiently while considering costs,
       |configurations, and application runtime, we recommend using ${instances.map(htmlBold).mkString(" and ")}
       |instances. Below, you'll find further details regarding the cluster and its resources.
       |""".stripMargin

  override def htmlResources: String = htmlTable(
    List("Role", "Count", "Instance", "Cpu", "Memory", s"Storage ${htmlTextSmall("**")}"),
    List(
      List("master", "1", masterNode.instanceType, s"${masterNode.vCpu}", s"${masterNode.memoryGiB}GB", humanReadableBytes(baseStorage)),
      List("core", s"$coreNodeNum", coreNode.instanceType, s"${coreNode.vCpu}", s"${coreNode.memoryGiB}GB", humanReadableBytes(workerStorage))
    ), "table-bordered table-striped table-sm text-center"
  )

  override def htmlServiceNotes: Seq[String] = Seq(
    htmlTextSmall(s"* Costs include ${printDurationStr(EmrOnEc2ProvisioningMs)} for cluster provisioning"),
    htmlTextSmall(s"** Storage allocation: ${humanReadableBytes(baseStorage)} (OS) + ${humanReadableBytes(sparkStorage)} (Spark)")
  )

  override def exampleCreateIamRoles: String = "aws emr create-default-roles"

  override def exampleSubmitJob(appInfo: AppInfo, conf: SparkRuntime): String = {

    val epoch = System.currentTimeMillis()
    val emrRelease = AwsEmr.latestRelease()
    val stepName = htmlTextRed(s"spark-test-$epoch")
    val classifications = getClusterClassifications(
      sparkRuntimeConfigs, toMB(getNodeUsableMemory(coreNode))
    ).replaceAll("\n", "\n    ")

    val sparkCmd = appInfo.sparkCmd.get

    s"""# Get a random public subnet from default VPC
       |VPC_ID=$$(aws ec2 describe-vpcs | jq -r ".Vpcs[] | select(.IsDefault==true)|.VpcId")
       |SUBNET_ID=$$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$$VPC_ID" | jq -r ".Subnets[] | select(.MapPublicIpOnLaunch==true)|.SubnetId" | shuf | head -n 1)
       |
       |aws emr create-cluster \\
       |  --name "${htmlTextRed(s"spark-test-$epoch")}" \\
       |  --release-label "${htmlTextRed(emrRelease)}" \\
       |  --use-default-roles \\
       |  --applications Name=Spark \\
       |  --ec2-attributes SubnetId=$$SUBNET_ID \\
       |  --instance-groups \\
       |    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=${masterNode.instanceType} \\
       |    InstanceGroupType=CORE,InstanceCount=$coreNodeNum,InstanceType=${coreNode.instanceType} \\
       |  --steps Name="$stepName",Args=[${sparkCmd.submitEc2Step}],ActionOnFailure=CONTINUE,Jar="command-runner.jar",Type=CUSTOM_JAR \\
       |  --configurations '$classifications' \\
       |  --auto-terminate \\
       |  --tags Project=${htmlTextRed(s"spark-test-$epoch")}
       |""".stripMargin
  }

}

object EmrOnEc2Env {

  def getNodeUsableMemory(instance: EmrInstance): Long = {
    val nodeOsMemory = byteStringAsBytes(Config.EmrOnEc2ReservedOsMemoryGb)
    val nodeTotalMemory = byteStringAsBytes(s"${instance.memoryGiB}g")
    nodeTotalMemory - nodeOsMemory
  }

  def getClusterClassifications(sparkRuntimeConfigs: SparkRuntime, nodeManagerMemoryMb: Long): String = {
    s"""[
       |${getYarnClassification(nodeManagerMemoryMb)},
       |${sparkRuntimeConfigs.getSparkClassification}
       |]"""
      .stripMargin
  }

  def getYarnClassification(nodeManagerMemoryMb: Long): String = {
    s"""{
       |  "Classification": "yarn-site",
       |  "Properties": {
       |    "yarn.nodemanager.resource.memory-mb": "$nodeManagerMemoryMb",
       |    "yarn.scheduler.maximum-allocation-mb": "$nodeManagerMemoryMb"
       |  }
       |}""".stripMargin
  }

}