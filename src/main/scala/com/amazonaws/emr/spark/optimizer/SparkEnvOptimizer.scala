package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.{EbsDefaultStorage, SparkInstanceFamilies}
import com.amazonaws.emr.api.{AwsCosts, AwsPricing}
import com.amazonaws.emr.api.AwsCosts.{EmrCosts, EmrOnEc2Cost, EmrOnEksCost}
import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, ComputeType, EmrInstance, VolumeType}
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.{AppContext, OptimalTypes}
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.adjustToNearestValidWorkerSpec
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, EnvironmentName, SERVERLESS}
import com.amazonaws.emr.spark.models.runtime.SparkRuntime.getMemoryWithOverhead
import com.amazonaws.emr.spark.models.runtime.{ContainerRequest, EmrEnvironment, EmrOnEc2Env, EmrOnEksEnv, EmrServerlessEnv, ResourceRequest, SparkRuntime}
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, roundUp, toGB, toMB}
import org.apache.logging.log4j.scala.Logging

/**
 * = SparkEnvOptimizer =
 *
 * Provides environment-level optimization for Spark applications on Amazon EMR by evaluating
 * cost-efficiency and performance trade-offs across supported platforms: EC2, EKS, and Serverless.
 *
 * This class leverages Spark runtime configurations and simulation data to recommend optimal
 * deployment environments based on user-defined objectives such as cost reduction or time minimization.
 *
 * == Constructor Parameters ==
 * @param awsRegion    AWS region used to fetch instance and pricing data.
 * @param spotDiscount Fractional spot pricing discount to apply for EC2 and EKS (0.0 = no discount, 1.0 = free).
 *
 */
class SparkEnvOptimizer(appContext: AppContext, awsRegion: String, spotDiscount: Double) extends Logging {

  private val allInstances = AwsPricing
    .getEmrAvailableInstances(awsRegion)
    .filter(i => i.vCpu >= 2 && i.memoryGiB >= 8)

  private val emrOnEksUplift = AwsPricing.getEmrContainersPrice(awsRegion)
    .find(_.compute == ComputeType.EC2)
    .getOrElse(throw new IllegalStateException("No EC2 compute uplift found"))

  private val emrServerlessPricing = AwsPricing.getEmrServerlessPrice(awsRegion)

  private val ebsGbPerMonthCost = AwsPricing.getEbsServicePrice(awsRegion)
    .find(_.volumeType == EbsDefaultStorage)
    .map(_.price)
    .getOrElse(throw new IllegalStateException("No EBS service price found"))

  /**
   * Suggests the best-fit EMR environment based on target runtime, pricing, and optimization goals.
   *
   * @param sparkRuntime Estimated Spark runtime and resource requests.
   * @param environment  Target environment to optimize for (EC2, EKS, or SERVERLESS).
   * @param optType      Optimization type: CostOpt, TimeOpt, or PerformanceOpt.
   * @param simulations  Simulated execution data for runtime estimation.
   * @return A sorted list of environment configurations, lowest cost first.
   */
  def recommend(
    sparkRuntime: SparkRuntime,
    environment: EnvironmentName,
    optType: OptimalType,
    simulations: Seq[SimulationWithCores]
  ): List[EmrEnvironment] = {
    val environments = environment match {
      case EC2 | EKS => buildEc2Environment(sparkRuntime, environment, optType, simulations)
      case SERVERLESS => buildServerlessEnvironment(sparkRuntime, optType, simulations).toList
      case _ => Nil
    }
    environments.sortBy(_.costs.total)
  }

  /**
   * Internal method to construct an EC2 or EKS environment by evaluating Spark instance types.
   *
   * @param runtime      Spark runtime requirements.
   * @param env          Target environment (EC2 or EKS).
   * @param optType      Optimization type (CostOpt or TimeOpt).
   * @param simulations  Simulation data for this configuration.
   * @return List of possible environment configurations.
   */
  private def buildEc2Environment(
    runtime: SparkRuntime,
    env: EnvironmentName,
    optType: OptimalType,
    simulations: Seq[SimulationWithCores]
  ): List[EmrEnvironment] = {
    val driver = runtime.driverRequest
    val executors = runtime.executorsRequest

    val execCandidates = filterSparkInstances(allInstances, executors, optType)
      .map(i => evaluateInstance(i, runtime, executors, env))
      .filter(test => test.containersPerInstance >= 1 && test.costs.hardware != 0.0 && test.totalInstances >= 2)

    execCandidates.flatMap { execTest =>
      val driverCandidates = filterSparkInstances(allInstances, driver, optType)
        .filter(_.instanceFamily == execTest.i.instanceFamily)
        .map(i => evaluateInstance(i, runtime, driver, env))
        .filter(_.containersPerInstance >= 1)

      val driverTests = if (driverCandidates.nonEmpty) driverCandidates else List(execTest)

      driverTests.map { driverTest =>
        buildEnv(env, driverTest, execTest, runtime, simulations)
      }
    }
  }

  /**
   * Builds the final EC2/EKS environment configuration using selected instance types.
   *
   * @param env          Target environment.
   * @param driverTest   Selected driver instance evaluation.
   * @param execTest     Selected executor instance evaluation.
   * @param runtime      Spark runtime configuration.
   * @param simulations  Simulation list used for performance projection.
   * @return Environment configuration object.
   */
  private def buildEnv(
    env: EnvironmentName,
    driverTest: InstanceTest,
    execTest: InstanceTest,
    runtime: SparkRuntime,
    simulations: Seq[SimulationWithCores]
  ): EmrEnvironment = {
    val totalCosts = env match {
      case EC2 => EmrOnEc2Cost(
        driverTest.costs.total + execTest.costs.total,
        driverTest.costs.emr + execTest.costs.emr,
        driverTest.costs.hardware + execTest.costs.hardware,
        driverTest.costs.storage + execTest.costs.storage,
        awsRegion,
        spotDiscount
      )
      case EKS => EmrOnEksCost(
        driverTest.costs.total + execTest.costs.total,
        driverTest.costs.emr + execTest.costs.emr,
        driverTest.costs.hardware + execTest.costs.hardware,
        driverTest.costs.storage + execTest.costs.storage,
        awsRegion,
        spotDiscount
      )
    }

    env match {
      case EC2 => EmrOnEc2Env(driverTest.i, execTest.i, execTest.totalInstances, runtime, execTest.containersPerInstance,
        totalCosts.asInstanceOf[EmrOnEc2Cost], driverTest.request, execTest.request, execTest.resourceWaste, Some(simulations))
      case EKS => EmrOnEksEnv(driverTest.i, execTest.i, execTest.totalInstances, runtime, execTest.containersPerInstance,
        totalCosts.asInstanceOf[EmrOnEksCost], driverTest.request, execTest.request, execTest.resourceWaste, Some(simulations))
    }
  }

  /**
   * Evaluates a specific EC2 instance for compatibility with the given Spark request.
   *
   * @param i             Instance type being evaluated.
   * @param runtimeConfig Spark runtime (used for billing time).
   * @param request       Spark driver or executor container spec.
   * @param deployment    Target platform (EC2 or EKS).
   * @return InstanceTest summarizing cost, utilization, and capacity info.
   */
  private def evaluateInstance(
    i: EmrInstance,
    runtimeConfig: SparkRuntime,
    request: ResourceRequest,
    deployment: EnvironmentName
  ): InstanceTest = {
    val runtimeHrs = runtimeConfig.runtimeHrs(extraTimeMs = 30000)
    val memMB = toMB(EmrOnEc2Env.getNodeUsableMemory(i))
    val containerMem = getMemoryWithOverhead(request.memory)
    val byMem = memMB / toMB(containerMem)
    val byCpu = i.vCpu / request.cores
    val cpi = math.min(byMem, byCpu).toInt
    val totalInstances = roundUp(request.count.toDouble / cpi.toDouble)
    val costs = deployment match {
      case EC2 => AwsCosts.computeEmrOnEc2Costs(runtimeHrs, i, totalInstances, cpi, request, ebsGbPerMonthCost, spotDiscount, awsRegion)
      case EKS => AwsCosts.computeEmrOnEksCosts(runtimeHrs, i, totalInstances, cpi, request, ebsGbPerMonthCost, emrOnEksUplift, spotDiscount, awsRegion)
    }
    val resourceWaste = ResourceWaste(i.vCpu, request.cores * cpi, EmrOnEc2Env.getNodeUsableMemory(i), containerMem * cpi)

    InstanceTest(i, totalInstances, cpi, resourceWaste, costs, request)
  }

  /**
   * Builds a valid EMR Serverless environment if driver and executor specs match a supported worker configuration.
   *
   * @param runtime     Spark runtime and request info.
   * @param optType     Optimization goal (PerformanceOpt, CostOpt).
   * @param simulations Simulation results to embed into environment.
   * @return Optionally returns a serverless configuration if valid.
   */
  private def buildServerlessEnvironment(
    runtime: SparkRuntime,
    optType: OptimalType,
    simulations: Seq[SimulationWithCores]
  ): Option[EmrEnvironment] = {

    val driver = adjustToNearestValidWorkerSpec(runtime.driverCores, runtime.driverMemory)
    val executor = adjustToNearestValidWorkerSpec(runtime.executorCores, runtime.executorMemory)
    val numExec = runtime.executorsNum
    val runtimeHrs = runtime.runtimeHrs()
    val totalCores = driver.cpu + executor.cpu * numExec
    val totalMem = getMemoryWithOverhead(driver.memory) + getMemoryWithOverhead(executor.memory) * numExec
    val storage = runtime.executorStorageRequired
    val billableStorage = math.max(0, (storage - byteStringAsBytes("20g")) * numExec)

    val arch = optType match {
      case OptimalTypes.PerformanceOpt => ArchitectureType.X86_64
      case _ => ArchitectureType.ARM64
    }

    val cost = AwsCosts.computeEmrServerlessCosts(runtimeHrs, arch, totalCores, toGB(totalMem), toGB(billableStorage), emrServerlessPricing, awsRegion)
    val resourceWaste = ResourceWaste(0, 0, 0L, 0L)

    // Make sure that simulations exists after worker normalization
    val hasSimulations = simulations.exists(s => s.coresPerExecutor == executor.cpu && s.executorNum == numExec)
    val refinedSimulations = if(!hasSimulations) {
      logger.debug("Missing simulations for Serverless after worker normalization")
      SparkRuntimeEstimator.estimate(appContext, executor.cpu, Config.ExecutorsMaxTestsCount)
    } else simulations

    Some(
      EmrServerlessEnv(
        totalCores,
        totalMem,
        storage,
        arch,
        ContainerRequest(1, driver.cpu, driver.memory, byteStringAsBytes("20g")),
        ContainerRequest(numExec, executor.cpu, executor.memory, storage),
        runtime,
        cost,
        1,
        resourceWaste,
        Some(refinedSimulations)
      )
    )
  }

  /**
   * Filters EMR-compatible instance types based on Spark requirements and optimization goals.
   *
   * @param instances List of available instance types.
   * @param request   Spark resource request (cores, memory).
   * @param optType   Optimization type (e.g., PerformanceOpt may apply additional hardware filters).
   * @return Filtered list of valid instances.
   */
  private def filterSparkInstances(
    instances: List[EmrInstance],
    request: ResourceRequest,
    optType: OptimalType
  ): List[EmrInstance] = {

    val minMemoryGiB = toGB(request.memory)
    val requiredCores = request.cores

    val isPerfOpt = optType == OptimalTypes.PerformanceOpt
    val isSpotPreferred = spotDiscount > 0.0

    val maxAllowedSizeForSpot = 8  // 8xlarge

    instances.filter { instance =>

      // Base conditions
      val hasEnoughResources = instance.memoryGiB >= minMemoryGiB && instance.vCpu >= requiredCores
      val isNewGeneration = instance.currentGeneration
      val isSupportedFamily = SparkInstanceFamilies.exists(instance.instanceFamily.startsWith)
      val excludedSubFamily = !instance.instanceFamily.contains("a")

      // Performance conditions
      val isStorageGood = Set(VolumeType.NVME, VolumeType.SSD).contains(instance.volumeType)
      val hasGoodNetwork = instance.networkBandwidthGbps >= 10

      // Extract instance size from type (e.g., c6g.4xlarge → 4)
      val instanceSize = """(\d+)[xX]large""".r.findFirstMatchIn(instance.instanceType).map(_.group(1).toInt)
      val isAcceptableSizeForSpot = !isSpotPreferred || instanceSize.forall(_ <= maxAllowedSizeForSpot)

      val baseConditions = hasEnoughResources && isSupportedFamily && isNewGeneration && excludedSubFamily
      val perfConditions = !isPerfOpt || (hasGoodNetwork && isStorageGood)
      val spotConditions = !isSpotPreferred || (isAcceptableSizeForSpot)

      baseConditions && perfConditions && spotConditions
    }

  }

  /**
   * Represents the result of evaluating a specific EC2 instance type against a Spark resource request.
   *
   * @param i                    The EMR instance type evaluated.
   * @param totalInstances       Number of instances required.
   * @param containersPerInstance Number of Spark containers that can run on this instance.
   * @param resourceWaste        CPU and memory waste metrics.
   * @param costs                Computed cost breakdown (hardware, EMR, storage).
   * @param request              Original Spark request matched against.
   */
  private case class InstanceTest(
    i: EmrInstance,
    totalInstances: Int,
    containersPerInstance: Int,
    resourceWaste: ResourceWaste,
    costs: EmrCosts,
    request: ResourceRequest
  ) {
    override def toString: String =
      s"""|${i.instanceType} ($totalInstances x ${i.vCpu} / ${i.memoryGiB} GB ) cpi: $containersPerInstance costs: (${costs.hardware}, ${costs.emr}, ${costs.storage}) wasted: $resourceWaste""".stripMargin
  }

}

/**
 * Encapsulates the CPU and memory waste metrics for a given configuration.
 *
 * @param totalCpu     Total available CPU cores on instances.
 * @param usedCpu      Actual cores used by Spark.
 * @param totalMemory  Total available memory on instances.
 * @param usedMemory   Actual memory used by Spark containers.
 */
case class ResourceWaste(totalCpu: Int, usedCpu: Int, totalMemory: Long, usedMemory: Long) {

  /**
   * @return Percentage of unused CPU capacity.
   */
  def cpuWastePercent: Double = {
    if (totalCpu == 0) 0.0 else (1.0 - usedCpu.toDouble / totalCpu) * 100
  }

  /**
   * @return Percentage of unused memory capacity.
   */
  def memoryWastePercent: Double = {
    if (totalMemory == 0) 0.0 else (1.0 - usedMemory.toDouble / totalMemory) * 100
  }

  /**
   * @return Average of CPU and memory waste percentages.
   */
  def averageWastePercent: Double = {
    (cpuWastePercent + memoryWastePercent) / 2.0
  }

  override def toString: String = {
    f"CPU Waste: $cpuWastePercent%.2f%%, Memory Waste: $memoryWastePercent%.2f%%, Average: $averageWastePercent%.2f%%"
  }

}
