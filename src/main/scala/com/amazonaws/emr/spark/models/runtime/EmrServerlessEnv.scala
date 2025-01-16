package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.Config.{EmrServerlessFreeStorageGb, EmrServerlessRoleName}
import com.amazonaws.emr.api.AwsCosts.EmrServerlessCost
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, DefaultCurrency}
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.spark.models.runtime.SparkRuntime.getMemoryWithOverhead
import com.amazonaws.emr.utils.Constants.{HtmlSvgEmrServerless, LinkEmrServerlessArchDoc, LinkEmrServerlessJobRoleDoc, LinkEmrServerlessQuickStart}
import com.amazonaws.emr.utils.Formatter._

import scala.language.postfixOps

case class EmrServerlessEnv(
  totalCores: Int,
  totalMemory: Long,
  totalStorage: Long,
  architecture: ArchitectureType.Value,
  driver: ResourceRequest,
  executors: ResourceRequest,
  override val sparkRuntime: SparkRuntime,
  costs: EmrServerlessCost,
  // Due to Serverless work node size limit (https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html#worker-configs),
  // we may need to use bigger workers for higher memory, in this case spark.task.cpus can larger than 1
  task_cpus: Int = 1
) extends EmrEnvironment with HtmlBase {

  val freeStorage: Long = byteStringAsBytes(EmrServerlessFreeStorageGb)
  val billableStorage: Long = {
    if (executors.storage > freeStorage) executors.storage - freeStorage
    else 0
  }

  override def label: String = "Emr Serverless"

  override def description: String = "Run your Spark workloads on EMR Serverless"

  override def serviceIcon: String = HtmlSvgEmrServerless

  override def htmlDescription: String =
    s"""With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications.
       |Based on costs, configurations and application runtime, it's recommended to
       |run the job using <b>$architecture</b> as ${htmlLink("(ISA)", LinkEmrServerlessArchDoc)}.
       |Your application will use the following resources:
       |""".stripMargin

  override def htmlServiceNotes: Seq[String] = {
    val notes = Seq(
      htmlTextSmall(s"* Costs include additional Spark memory overhead"),
      htmlTextSmall(s"** Storage allocation: ${humanReadableBytes(freeStorage)} (Free) + ${humanReadableBytes(billableStorage)} (Paid)"))
    notes ++ (if (task_cpus > 1) {
      Seq(htmlTextSmall(s"*** Set spark.task.cpus to ${task_cpus}"))
    } else {
      Seq.empty[String]
    })
  }

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

  override def htmlExample(appInfo: AppInfo): String = {
    s"""1. (Optional) ${htmlLink("Create an IAM Job Role", LinkEmrServerlessJobRoleDoc)}
       |<br/><br/>
       |2. Create an Emr Serverless Application using the latest EMR release to submit your Spark job
       |${htmlCodeBlock(exampleRequirements, "bash")}
       |3. Review the parameters and submit the application
       |${htmlCodeBlock(exampleSubmitJob(appInfo, sparkRuntime), "bash")}
       |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR Serverless", LinkEmrServerlessQuickStart)}
       |in the AWS Documentation.</p>""".stripMargin
  }

  private def exampleRequirements: String = {
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

  private def exampleSubmitJob(appInfo: AppInfo, conf: SparkRuntime): String = {

    val sparkCmd = appInfo.sparkCmd.get
    val executorStorageStr = s"${toGB(executors.storage)}G"
    val classParam = if (sparkCmd.isScala) s"--class ${htmlTextRed(sparkCmd.appMainClass)} " else ""
    val arguments = if (sparkCmd.appArguments.nonEmpty) {
      s"""\n        "entryPointArguments": [\n${htmlTextRed(sparkCmd.appArguments.mkString("          \"", "\",\n          \"", "\""))}],"""
    } else ""

    s"""# Specify an IAM role for the job
       |ROLE_NAME="${htmlTextRed(EmrServerlessRoleName)}"
       |ROLE_ARN=$$(aws iam get-role --role-name $$ROLE_NAME| jq -r .Role.Arn)
       |
       |# Submit job
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
       |      "applicationConfiguration": [
       |        {
       |          "classification": "spark",
       |          "properties": {
       |            "dynamicAllocationOptimization": "true"
       |          }
       |        }
       |      ]
       |    }'
       |""".stripMargin
  }

}

object EmrServerlessEnv {

  // Worker configurations based on the documentation
  // https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html#worker-configs
  private val WorkerSupportedConfig: List[WorkerNodeSpec] = List(
    WorkerNodeSpec(1, 2, 8, 1, 200),
    WorkerNodeSpec(2, 4, 16, 1, 200),
    WorkerNodeSpec(4, 8, 30, 1, 200),
    WorkerNodeSpec(8, 16, 60, 4, 200),
    WorkerNodeSpec(16, 32, 120, 8, 200)
  )

  def isValidConfig(sparkRuntime: SparkRuntime): Boolean =
    WorkerSupportedConfig.exists(spec =>
      sparkRuntime.executorCores == spec.cpu &&
        sparkRuntime.executorMemory <= byteStringAsBytes(s"${spec.maxMemoryGB}g")
    )

  def normalizeSparkConfigs(sparkRuntime: SparkRuntime): SparkRuntime = {
    val normalizedDriver = findWorkerByCpuMemory(sparkRuntime.driverCores, sparkRuntime.driverMemory)
    val normalizedExecutor = findWorkerByCpuMemory(sparkRuntime.executorCores, sparkRuntime.executorMemory)

    sparkRuntime.copy(
      driverCores = normalizedDriver.cpu,
      driverMemory = normalizedDriver.memory,
      executorCores = normalizedExecutor.cpu,
      executorMemory = normalizedExecutor.memory
    )
  }

  def findWorkerByCpuMemory(cores: Int, memory: Long): WorkerNode = {

    // Determine the appropriate CPU configuration
    val cpuReq = cores match {
      case _ if cores >= WorkerSupportedConfig.last.cpu => WorkerSupportedConfig.last.cpu
      case _ if cores <= WorkerSupportedConfig.head.cpu => WorkerSupportedConfig.head.cpu
      case _ => cores
    }

    // Convert memory to GB and determine the appropriate memory configuration
    val memoryGb = asGB(memory).toInt
    val memoryGbReq = memoryGb match {
      case _ if memoryGb >= WorkerSupportedConfig.last.maxMemoryGB => WorkerSupportedConfig.last.maxMemoryGB
      case _ if memoryGb <= WorkerSupportedConfig.head.minMemoryGB => WorkerSupportedConfig.head.minMemoryGB
      case _ => memoryGb
    }

    // Filter configurations based on memory and CPU requirements
    val validConfigs = WorkerSupportedConfig.filter(cfg =>
      cfg.cpu == cpuReq && memoryGbReq >= cfg.minMemoryGB && memoryGbReq <= cfg.maxMemoryGB
    )

    val validCpuConfigs = WorkerSupportedConfig.filter(cfg =>
      cfg.cpu == cpuReq && memoryGbReq <= cfg.maxMemoryGB
    )

    val validMemConfigs = WorkerSupportedConfig.filter(cfg =>
      memoryGbReq <= cfg.maxMemoryGB
    )

    // Select the appropriate WorkerNode based on the valid configurations
    validConfigs.headOption
      .map(cfg => WorkerNode(cfg.cpu, byteStringAsBytes(s"${memoryGbReq}g"), 0L))
      .orElse(validCpuConfigs.headOption.map(cfg => {
        val adjustedMemory = math.max(memoryGbReq, cfg.minMemoryGB)
        WorkerNode(cfg.cpu, byteStringAsBytes(s"${adjustedMemory}g"), 0L)
      }))
      .orElse(validMemConfigs.headOption.map(cfg =>
        WorkerNode(cfg.cpu, byteStringAsBytes(s"${memoryGbReq}g"), 0L)
      ))
      .getOrElse(
        WorkerNode(WorkerSupportedConfig.last.cpu, byteStringAsBytes(s"${WorkerSupportedConfig.last.maxMemoryGB}g"), 0L)
      )
  }

}

case class WorkerNode(cpu: Int, memory: Long, storage: Long)

case class WorkerNodeSpec(cpu: Int, minMemoryGB: Int, maxMemoryGB: Int, memoryIncrementGB: Int, maxStorageGB: Int)
