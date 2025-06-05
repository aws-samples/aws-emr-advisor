package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.Config.{EmrServerlessFreeStorageGb, EmrServerlessRoleName}
import com.amazonaws.emr.api.AwsCosts.EmrServerlessCost
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, DefaultCurrency}
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.spark.models.runtime.SparkRuntime.getMemoryWithOverhead
import com.amazonaws.emr.spark.optimizer.ResourceWaste
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
  task_cpus: Int = 1,
  resources: ResourceWaste,
  simulations: Option[Seq[SimulationWithCores]]
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
       |${htmlCodeBlock(exampleRequirements(appInfo), "bash")}
       |3. Review the parameters and submit the application
       |${htmlCodeBlock(exampleSubmitJob(appInfo, sparkRuntime), "bash")}
       |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR Serverless", LinkEmrServerlessQuickStart)}
       |in the AWS Documentation.</p>""".stripMargin
  }

  private def exampleRequirements(appInfo: AppInfo): String = {
    val epoch = System.currentTimeMillis()
    val latestRelease = appInfo.latestEmrRelease(awsRegion)
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
       |        "sparkSubmitParameters": "$classParam${conf.configurationStr} --conf spark.emr-serverless.executor.disk=$executorStorageStr"
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

  override def toString: String =
    s"""|-----------------
        |Total: ${costs.total}(ec2: ${costs.hardware} emr: ${costs.emr} storage: ${costs.storage})
        |Waste: AVG ${resources.averageWastePercent} CPU ${resources.cpuWastePercent} Mem ${resources.memoryWastePercent}
        |-----------------""".stripMargin

}

object EmrServerlessEnv {

  // Worker configurations based on the documentation
  // https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html#worker-configs
  val WorkerSupportedConfig: List[WorkerNodeSpec] = List(
    WorkerNodeSpec(1, 2, 8, 1, 200),
    WorkerNodeSpec(2, 4, 16, 1, 200),
    WorkerNodeSpec(4, 8, 30, 1, 200),
    WorkerNodeSpec(8, 16, 60, 4, 200),
    WorkerNodeSpec(16, 32, 120, 8, 200)
  )

  /**
   * Validates whether a given SparkRuntime configuration complies with EMR Serverless worker constraints.
   *
   * EMR Serverless supports a fixed set of worker configurations, each defined by a specific number of vCPUs
   * and a valid memory range with specific increments. This method checks if both the driver and executor
   * resource configurations in the SparkRuntime match any of the supported worker specifications.
   *
   * Validation criteria:
   *   - CPU cores must match one of the supported WorkerNodeSpec configurations.
   *   - Memory must fall within the allowed range [minMemoryGB, maxMemoryGB] for the given CPU tier.
   *   - Memory must be a multiple of the memory increment defined for that CPU tier.
   *
   * @param runtime The SparkRuntime instance to validate.
   * @return true if both driver and executor configurations are valid for EMR Serverless; false otherwise.
   */
  def isValidEmrServerlessRuntime(runtime: SparkRuntime): Boolean = {
    def isValidWorker(cpu: Int, memory: Long): Boolean = {
      WorkerSupportedConfig.exists { spec =>
        spec.cpu == cpu &&
          asGB(memory) >= spec.minMemoryGB &&
          asGB(memory) <= spec.maxMemoryGB &&
          asGB(memory) % spec.memoryIncrementGB == 0
      }
    }

    val driverValid = isValidWorker(runtime.driverCores, runtime.driverMemory)
    val executorValid = isValidWorker(runtime.executorCores, runtime.executorMemory)

    driverValid && executorValid
  }

  /**
   * Normalizes a SparkRuntime configuration to ensure it is compatible with EMR Serverless.
   *
   * EMR Serverless enforces constraints on worker node configurations—each valid configuration specifies a fixed
   * number of vCPUs and a bounded, incremented memory range. This method adjusts the driver and executor resource
   * configurations to the nearest valid EMR Serverless-supported worker configuration.
   *
   * Adjustments include:
   *   - Aligning CPU cores to a supported tier.
   *   - Clamping memory to the nearest allowed range for the selected core tier.
   *   - Rounding memory to a valid increment within the bounds.
   *
   * @param runtime The original (potentially invalid) SparkRuntime configuration.
   * @return A new SparkRuntime instance with driver and executor configurations adjusted for EMR Serverless.
   */
  def normalizeEmrServerlessRuntime(runtime: SparkRuntime): SparkRuntime = {
    val normalizedDriver: WorkerNode = adjustToNearestValidWorkerSpec(runtime.driverCores, runtime.driverMemory)
    val normalizedExecutor: WorkerNode = adjustToNearestValidWorkerSpec(runtime.executorCores, runtime.executorMemory)

    runtime.copy(
      driverCores = normalizedDriver.cpu,
      driverMemory = normalizedDriver.memory,
      executorCores = normalizedExecutor.cpu,
      executorMemory = normalizedExecutor.memory
    )
  }

  /**
   * Adjusts a requested (CPU, memory) configuration to the closest valid EMR Serverless worker spec.
   *
   * EMR Serverless supports discrete worker sizes defined by:
   *   - Fixed CPU core count
   *   - Min and max memory (in GB)
   *   - Memory increments (e.g., must be multiple of 1, 4, 8 GB etc.)
   *
   * If no exact CPU match is found, it selects the next-highest tier and chooses the memory size
   * within that tier that most closely approximates the requested memory (respecting increment and bounds).
   *
   * @param requestedCpu Requested number of vCPUs
   * @param requestedMemoryBytes Requested memory in bytes
   * @param supportedConfigs List of valid worker specs (defaults to EMR Serverless supported set)
   * @return WorkerNode that conforms to a supported worker spec
   */
  def adjustToNearestValidWorkerSpec(
    requestedCpu: Int,
    requestedMemoryBytes: Long,
    supportedConfigs: List[WorkerNodeSpec] = WorkerSupportedConfig
  ): WorkerNode = {

    val requestedMemoryGB = asGB(requestedMemoryBytes).toInt

    // Try exact CPU match first
    supportedConfigs.find(_.cpu == requestedCpu) match {
      case Some(spec) =>
        val adjustedMem = normalizeMemory(requestedMemoryGB, spec)
        WorkerNode(spec.cpu, byteStringAsBytes(s"${adjustedMem}g"), 0L)

      case None =>
        // Fall back to higher CPU tier and find closest memory fit
        val candidates = supportedConfigs.filter(_.cpu >= requestedCpu)
        val bestFit = candidates.minBy { spec =>
          val mem = normalizeMemory(requestedMemoryGB, spec)
          math.abs(mem - requestedMemoryGB)
        }
        val adjustedMem = normalizeMemory(requestedMemoryGB, bestFit)
        WorkerNode(bestFit.cpu, byteStringAsBytes(s"${adjustedMem}g"), 0L)
    }
  }

  /**
   * Rounds a requested memory value to the nearest valid memory increment within the spec's bounds.
   */
  private def normalizeMemory(requestedGB: Int, spec: WorkerNodeSpec): Int = {
    val min = spec.minMemoryGB
    val max = spec.maxMemoryGB
    val inc = spec.memoryIncrementGB

    val clamped = requestedGB.max(min).min(max)

    val adjusted = ((clamped - min + inc / 2) / inc) * inc + min
    adjusted.min(max)
  }

}

case class WorkerNode(cpu: Int, memory: Long, storage: Long)

case class WorkerNodeSpec(cpu: Int, minMemoryGB: Int, maxMemoryGB: Int, memoryIncrementGB: Int, maxStorageGB: Int)
