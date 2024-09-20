package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.Config.{EmrServerlessFreeStorageGb, EmrServerlessRoleName}
import com.amazonaws.emr.utils.Constants.LinkEmrServerlessArchDoc
import com.amazonaws.emr.api.AwsCosts.EmrServerlessCost
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, DefaultCurrency}
import com.amazonaws.emr.report.HtmlReport.{htmlGroupListWithFloat, htmlLink, htmlTable, htmlTextRed, htmlTextSmall}
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.spark.models.runtime.SparkRuntime.getMemoryWithOverhead
import com.amazonaws.emr.utils.Formatter._

case class EmrServerlessEnv(
  totalCores: Int,
  totalMemory: Long,
  totalStorage: Long,
  architecture: ArchitectureType.Value,
  driver: ResourceRequest,
  executors: ResourceRequest,
  costs: EmrServerlessCost
) extends EmrEnvironment {

  val freeStorage: Long = byteStringAsBytes(EmrServerlessFreeStorageGb)
  val billableStorage: Long = {
    if (executors.storage > freeStorage) executors.storage - freeStorage
    else 0
  }

  override def label: String = "Emr Serverless"

  override def htmlDescription: String =
    s"""With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications.
       |Based on costs, configurations and application runtime, it's recommended to
       |run the job using <b>$architecture</b> as ${htmlLink("(ISA)", LinkEmrServerlessArchDoc)}.
       |Your application will use the following resources:
       |""".stripMargin

  override def htmlServiceNotes: Seq[String] = Seq(
    htmlTextSmall(s"* Costs include additional Spark memory overhead"),
    htmlTextSmall(s"** Storage allocation: ${humanReadableBytes(freeStorage)} (Free) + ${humanReadableBytes(billableStorage)} (Paid)")
  )

  override def htmlCosts: String = htmlGroupListWithFloat(Seq(
    (s"""Cores Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.cpu)} $DefaultCurrency"),
    (s"""Memory Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.memory)} $DefaultCurrency"),
    (s"""Storage Costs ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.storage)} $DefaultCurrency"),
    (s"""<B>Total Costs</B> ${htmlTextSmall("*")}""", s"${"%.2f".format(costs.cpu + costs.memory + costs.storage)} $DefaultCurrency")
  ), "list-group-flush mb-2", "px-0")

  override def htmlResources: String = htmlTable(
    List("Role", "Count", "Cpu", "Memory", s"Storage ${htmlTextSmall("**")}"),
    List(
      List("driver", "1", s"${driver.cores}", s"${humanReadableBytes(driver.memory)}", s"${humanReadableBytes(driver.storage)}"),
      List("executors", s"${executors.count}", s"${executors.cores}", s"${humanReadableBytes(executors.memory)}", s"${humanReadableBytes(executors.storage)}")
    ), "table-bordered table-striped table-sm text-center")

  override def exampleRequirements(appInfo: AppInfo): String = {
    val epoch = System.currentTimeMillis()
    val latestRelease = AwsEmr.latestRelease()
    val totalMemoryWithOverhead = {
      getMemoryWithOverhead(driver.memory, 0.15) + getMemoryWithOverhead(executors.memory, 0.15) * executors.count
    }

    s"""APP_ID=$$(aws emr-serverless create-application \\
       |    --type "SPARK" \\
       |    --name "${htmlTextRed(s"spark-test-$epoch")}" \\
       |    --architecture ${htmlTextRed(architecture.toString)} \\
       |    --release-label ${htmlTextRed(latestRelease)} \\
       |    --maximum-capacity '{
       |      "cpu": "${totalCores}vCPU",
       |      "memory": "${toGB(totalMemoryWithOverhead)}GB",
       |      "disk": "${toGB(totalStorage) + 1}GB"
       |    }' | jq -r .applicationId)
       |""".stripMargin
  }

  override def exampleSubmitJob(appInfo: AppInfo, conf: SparkRuntime): String = {

    val sparkCmd = appInfo.sparkCmd.get
    val executorStorageStr = s"${toGB(executors.storage)}G"
    val classParam = if (sparkCmd.isScala) s"--class ${htmlTextRed(sparkCmd.appMainClass)} " else ""
    val arguments = if (sparkCmd.appArguments.nonEmpty) {
      s"""\n"entryPointArguments": [${htmlTextRed(sparkCmd.appArguments.mkString("\"", "\",\"", "\""))}],"""
    } else ""

    s"""# Specify an IAM role for the job
       |ROLE_NAME="${htmlTextRed(EmrServerlessRoleName)}"
       |ROLE_ARN=$$(aws iam get-role --role-name $$ROLE_NAME| jq -r .Role.Arn)
       |
       |aws emr-serverless start-job-run \\
       |    --application-id $$APP_ID \\
       |    --execution-role-arn $$ROLE_ARN \\
       |    --job-driver '{
       |      "sparkSubmit": {
       |        "entryPoint": "${htmlTextRed(sparkCmd.appScriptJarPath)}",$arguments
       |        "sparkSubmitParameters": "$classParam${conf.sparkMainConfString} --conf spark.emr-serverless.executor.disk=$executorStorageStr"
       |      }
       |    }' \\
       |    --configuration-overrides '{
       |      "applicationConfiguration": [{
       |      "classification": "spark",
       |      "properties": {
       |       "dynamicAllocationOptimization": "true"
       |      }
       |     }]
       |    }'
       |""".stripMargin
  }

}

object EmrServerlessEnv {

  // https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html#worker-configs
  private val WorkerSupportedConfig: List[WorkerNodeSpec] = List(
    WorkerNodeSpec(1, 2, 8, 1, 200),
    WorkerNodeSpec(2, 4, 16, 1, 200),
    WorkerNodeSpec(4, 8, 30, 1, 200),
    WorkerNodeSpec(8, 16, 60, 4, 200),
    WorkerNodeSpec(16, 32, 120, 8, 200),
  )

  def findWorkerByCpuMemory(cores: Int, memory: Long): WorkerNode = {

    val cpuReq = {
      if (cores >= WorkerSupportedConfig.last.cpu) WorkerSupportedConfig.last.cpu
      else if (cores <= WorkerSupportedConfig.head.cpu) WorkerSupportedConfig.head.cpu
      else cores
    }

    val memoryGb = asGB(memory).toInt
    val memoryGbReq = {
      if (memoryGb >= WorkerSupportedConfig.last.maxMemoryGB) WorkerSupportedConfig.last.maxMemoryGB
      else if (memoryGb <= WorkerSupportedConfig.head.minMemoryGB) WorkerSupportedConfig.head.minMemoryGB
      else memoryGb
    }

    // check if we're over boundaries
    val validConfigs = WorkerSupportedConfig
      .filter(memoryGbReq <= _.maxMemoryGB)
      .filter(memoryGbReq >= _.minMemoryGB)
      .filter(cpuReq == _.cpu)

    // check if we're over boundaries
    val validCpuConfigs = WorkerSupportedConfig
      .filter(memoryGbReq <= _.maxMemoryGB)
      .filter(cpuReq == _.cpu)

    val validMemConfigs = WorkerSupportedConfig
      .filter(memoryGbReq <= _.maxMemoryGB)

    if (validConfigs.nonEmpty) {
      WorkerNode(
        cpuReq,
        byteStringAsBytes(s"${memoryGbReq}g"),
        0L
      )
    } else if (validCpuConfigs.nonEmpty) {
      val tmpMem = {
        if (memoryGbReq <= validCpuConfigs.head.minMemoryGB) validCpuConfigs.head.minMemoryGB
        else memoryGbReq
      }
      WorkerNode(
        validCpuConfigs.head.cpu,
        byteStringAsBytes(s"${tmpMem}g"),
        0L
      )
    } else if (validMemConfigs.nonEmpty) {
      WorkerNode(
        validMemConfigs.head.cpu,
        byteStringAsBytes(s"${memoryGbReq}g"),
        0L
      )
    } else {
      WorkerNode(
        WorkerSupportedConfig.last.cpu,
        byteStringAsBytes(s"${WorkerSupportedConfig.last.maxMemoryGB}g"),
        0L
      )
    }

  }

}

case class WorkerNode(cpu: Int, memory: Long, storage: Long)

case class WorkerNodeSpec(cpu: Int, minMemoryGB: Int, maxMemoryGB: Int, memoryIncrementGB: Int, maxStorageGB: Int)
