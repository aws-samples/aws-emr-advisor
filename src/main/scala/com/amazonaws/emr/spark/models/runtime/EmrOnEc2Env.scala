package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.{EmrOnEc2MinStorage, EmrOnEc2ProvisioningMs}
import com.amazonaws.emr.api.AwsCosts.EmrOnEc2Cost
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.api.AwsPricing.EmrInstance
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.spark.models.runtime.EmrOnEc2Env.{getClusterClassifications, getNodeUsableMemory}
import com.amazonaws.emr.utils.Constants.{HtmlSvgEmrOnEc2, LinkEmrOnEc2IamRoles, LinkEmrOnEc2QuickStart}
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes, printDurationStr, toMB}
import software.amazon.awssdk.regions.Region

case class EmrOnEc2Env(
  masterNode: EmrInstance,
  coreNode: EmrInstance,
  coreNodeNum: Int,
  sparkRuntime: SparkRuntime,
  yarnContainersPerInstance: Int,
  costs: EmrOnEc2Cost,
  driver: ResourceRequest,
  executors: ResourceRequest
) extends Ordered[EmrOnEc2Env] with EmrEnvironment with HtmlBase {

  private val baseStorage = byteStringAsBytes(EmrOnEc2MinStorage)
  private val sparkStorage = yarnContainersPerInstance * executors.storage
  private val workerStorage = baseStorage + sparkStorage

  def compare(that: EmrOnEc2Env): Int = this.costs.total compare that.costs.total

  override def label: String = "Emr On Ec2"

  override def description: String = "Managed Hadoop running on Amazon EC2"

  override def serviceIcon: String = HtmlSvgEmrOnEc2

  override def instances: List[String] = List(masterNode.instanceType, coreNode.instanceType)

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

  override def htmlExample(appInfo: AppInfo): String = {
    s"""1. (Optional) Create default ${htmlLink("IAM Roles for EMR", LinkEmrOnEc2IamRoles)}
       |${htmlCodeBlock(exampleCreateIamRoles, "bash")}
       |2. Review the parameters and launch an EC2 cluster to test the configurations
       |${htmlCodeBlock(exampleSubmitJob(appInfo), "bash")}
       |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR", LinkEmrOnEc2QuickStart)}
       |in the AWS Documentation.</p>""".stripMargin
  }

  private def exampleCreateIamRoles: String = "aws emr create-default-roles"

  private def exampleSubmitJob(appInfo: AppInfo): String = {

    val epoch = System.currentTimeMillis()
    val emrRelease = AwsEmr.latestRelease(awsRegion)
    val stepName = htmlTextRed(s"spark-test-$epoch")
    val classifications = getClusterClassifications(
      sparkRuntime, toMB(getNodeUsableMemory(coreNode))
    ).replaceAll("\n", "\n    ")

    val sparkCmd = appInfo.sparkCmd.get

    s"""# Get a random public subnet from default VPC
       |VPC_ID=$$(aws ec2 describe-vpcs | jq -r ".Vpcs[] | select(.IsDefault==true) | .VpcId")
       |SUBNET_ID=$$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$$VPC_ID" | \\
       |  jq -r '.Subnets[] | select(.MapPublicIpOnLaunch == true) | .SubnetId' | \\
       |  shuf | head -n 1)
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
       |  --configurations '$classifications' \\
       |  --auto-terminate \\
       |  --tags Project=${htmlTextRed(s"spark-test-$epoch")} \\
       |  --steps '[
       |    {
       |      "Name": "$stepName",
       |      "Args": [
       |        ${sparkCmd.submitEc2Step}
       |      ],
       |      "ActionOnFailure": "CONTINUE",
       |      "Jar": "command-runner.jar",
       |      "Type": "CUSTOM_JAR"
       |    }
       |  ]'
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

  private def getYarnClassification(nodeManagerMemoryMb: Long): String = {
    s"""{
       |  "Classification": "yarn-site",
       |  "Properties": {
       |    "yarn.nodemanager.resource.memory-mb": "$nodeManagerMemoryMb",
       |    "yarn.scheduler.maximum-allocation-mb": "$nodeManagerMemoryMb"
       |  }
       |}""".stripMargin
  }

}