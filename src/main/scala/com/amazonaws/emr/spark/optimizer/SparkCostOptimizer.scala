package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config._
import com.amazonaws.emr.api.AwsCosts.{EmrCosts, EmrOnEc2Cost, EmrOnEksCost}
import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, ComputeType, EmrInstance, VolumeType}
import com.amazonaws.emr.api.{AwsCosts, AwsPricing}
import com.amazonaws.emr.spark.analyzer.SimulationWithCores
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.OptimalTypes.{CostOpt, OptimalType, TimeOpt}
import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.{findWorkerByCpuMemory, isValidConfig, normalizeSparkConfigs}
import com.amazonaws.emr.spark.models.runtime.Environment.{EC2, EKS, EnvironmentName, SERVERLESS}
import com.amazonaws.emr.spark.models.runtime.SparkRuntime.getMemoryWithOverhead
import com.amazonaws.emr.spark.models.runtime._
import com.amazonaws.emr.utils.Formatter._
import org.apache.logging.log4j.scala.Logging

class SparkCostOptimizer(awsRegion: String, spotDiscount: Double) extends Logging {

  // get instances with corresponding price
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

  def findOptCostSparkConf(
    appContext: AppContext,
    simulationList: Seq[SimulationWithCores],
    environment: EnvironmentName,
    optType: OptimalType,
    expectedDuration: Option[Long] = None
  ): Option[SparkRuntime] = {

    val sparkConfs = expectedDuration
      .map(t => simulationList.filter(_.appRuntimeEstimate.estimatedAppTimeMs <= t))
      .getOrElse(simulationList)
      .map(SparkBaseOptimizer.createSparkRuntime(appContext, _))

    val sparkRuntime = environment match {
      case EC2 | EKS =>
        val optimizedConfigs = sparkConfs.map(c => findOptEnv(c, environment, optType).get)
        Some(optimizedConfigs.minBy(_.costs.total).sparkRuntime)
      case SERVERLESS =>
        val optimalConfig = sparkConfs
          .filter(isValidConfig)
          .map(normalizeSparkConfigs)
          .flatMap(findEmrServerlessOptimal(_, optType))
          .minBy(_.costs.total)
          .sparkRuntime
        Some(optimalConfig)
      case _ => None
    }

    sparkRuntime
  }

  def findOptCostEnv(
    sparkRuntime: SparkRuntime,
    environment: EnvironmentName,
    optType: OptimalType
  ): Option[EmrEnvironment] =
    environment match {
      case EC2 | EKS => findOptEnv(sparkRuntime, environment, optType)
      case SERVERLESS => findEmrServerlessOptimal(sparkRuntime, optType)
      case _ => None
    }

  // ========================================================================
  // EMR EC2 / EKS
  // ========================================================================
  private def findOptEnv(
    sparkRuntimeConfigs: SparkRuntime,
    deployment: EnvironmentName,
    optType: OptimalType
  ): Option[EmrEnvironment] = {

    val driver = sparkRuntimeConfigs.getDriverContainer
    val executors = sparkRuntimeConfigs.getExecutorContainer

    // evaluate instances for spark executors
    val workerNodes = {
      deployment match {
        case EC2 =>
          filterSparkInstances(allInstances, executors, optType)
            .map(i => evaluate(i, sparkRuntimeConfigs, executors, ebsGbPerMonthCost, EC2))
            .filter(_.containersPerInstance >= 1)
            .filter(s => s.totalInstances >= EmrOnEc2ClusterMinNodes)
            .filter(_.containersPerInstance <= EmrOnEc2MaxContainersPerInstance)

        case EKS =>
          filterSparkInstances(allInstances, executors, optType)
            .map(i => evaluate(i, sparkRuntimeConfigs, executors, ebsGbPerMonthCost, Environment.EKS))
            .filter(_.containersPerInstance >= 1)
            .filter(_.containersPerInstance <= EmrOnEksMaxPodsPerInstance)

        case _ => Nil

      }
    }.sortBy(_.costs.total).take(200)

    val workerNode: InstanceTest = workerNodes.minBy(_.costs.total)
    val workersInstanceGen = workerNode.i.instanceFamily.split("\\D+").find(_.nonEmpty).getOrElse("0")

    // evaluate instances for spark driver
    val driverNodeTmp = {
      deployment match {
        case EC2 =>
          filterSparkInstances(allInstances, driver, CostOpt)
            .filter(_.instanceFamily.contains(workersInstanceGen))
            .map(i => evaluate(i, sparkRuntimeConfigs, driver, ebsGbPerMonthCost, EC2))
            .filter(_.containersPerInstance >= 1)
        case EKS =>
          filterSparkInstances(allInstances, driver, CostOpt)
            .filter(_.instanceFamily.contains(workersInstanceGen))
            .map(i => evaluate(i, sparkRuntimeConfigs, driver, ebsGbPerMonthCost, Environment.EKS))
            .filter(_.containersPerInstance >= 1)
        case _ => Nil
      }
    }.sortBy(_.costs.total).take(40)


    val driverNode = driverNodeTmp.minBy(_.costs.total)

    // return environment
    deployment match {
      case EC2 =>
        Some(
          EmrOnEc2Env(
            driverNode.i,
            workerNode.i,
            workerNode.totalInstances,
            sparkRuntimeConfigs,
            workerNode.containersPerInstance,
            EmrOnEc2Cost(
              driverNode.costs.total + workerNode.costs.total,
              driverNode.costs.emr + workerNode.costs.emr,
              driverNode.costs.hardware + workerNode.costs.hardware,
              driverNode.costs.storage + workerNode.costs.storage,
              awsRegion,
              spotDiscount
            ),
            driver,
            executors
          )
        )
      case EKS =>
        Some(
          EmrOnEksEnv(
            driverNode.i,
            workerNode.i,
            workerNode.totalInstances,
            sparkRuntimeConfigs,
            workerNode.containersPerInstance,
            EmrOnEksCost(
              driverNode.costs.total + workerNode.costs.total,
              driverNode.costs.emr + workerNode.costs.emr,
              driverNode.costs.hardware + workerNode.costs.hardware,
              driverNode.costs.storage + workerNode.costs.storage,
              awsRegion,
              spotDiscount
            ),
            driver,
            executors
          )
        )
    }

  }

  private def evaluate(
    i: EmrInstance,
    runtimeConfig: SparkRuntime,
    request: ResourceRequest,
    ebsGbPerMonthCost: Double,
    deployment: EnvironmentName
  ): InstanceTest = {

    val runtimeHrs = deployment match {
      case EC2 => runtimeConfig.runtimeHrs(extraTimeMs = EmrOnEc2ProvisioningMs)
      case EKS => runtimeConfig.runtimeHrs(extraTimeMs = EmrOnEksProvisioningMs)
      case SERVERLESS => Double.MaxValue
    }

    val availableMemoryMB = deployment match {
      case EC2 => toMB(EmrOnEc2Env.getNodeUsableMemory(i))
      case EKS => i.memoryGiB * 1024
      case SERVERLESS => Int.MinValue
    }

    // add spark overhead memory
    val containerMemory = getMemoryWithOverhead(request.memory)

    val containersPerInstanceByMem = availableMemoryMB / toMB(containerMemory)
    val containersPerInstanceByCpu = i.vCpu / request.cores
    val containersPerInstance = containersPerInstanceByMem.min(containersPerInstanceByCpu)

    val totalInstances = {
      if (containersPerInstance >= request.count) 1
      else roundUp(request.count.toDouble / containersPerInstance.toDouble)
    }

    // compute wasted resources
    val wastedCpu = i.vCpu - (containersPerInstance * request.cores)
    val wastedMemory = availableMemoryMB - (containersPerInstance * containerMemory)

    // compute costs
    val costs = deployment match {
      case EC2 => AwsCosts.computeEmrOnEc2Costs(runtimeHrs, i, totalInstances, containersPerInstance, request, ebsGbPerMonthCost, spotDiscount, awsRegion)
      case EKS => AwsCosts.computeEmrOnEksCosts(runtimeHrs, i, totalInstances, containersPerInstance, request, ebsGbPerMonthCost, emrOnEksUplift, spotDiscount, awsRegion)
      case _ => EmrOnEc2Cost(0, 0, 0, 0, awsRegion)
    }
    InstanceTest(i, totalInstances, containersPerInstance, costs, wastedCpu, wastedMemory)
  }

  // ========================================================================
  // EMR Serverless
  // ========================================================================
  private def findEmrServerlessOptimal(
    sparkRuntimeConfigs: SparkRuntime,
    optType: OptimalType
  ): Option[EmrEnvironment] = {

    val driverNode = findWorkerByCpuMemory(sparkRuntimeConfigs.driverCores, sparkRuntimeConfigs.driverMemory)
    val executorNode = findWorkerByCpuMemory(sparkRuntimeConfigs.executorCores, sparkRuntimeConfigs.executorMemory)
    val executorsNum = sparkRuntimeConfigs.executorsNum

    val freeStorage = byteStringAsBytes(EmrServerlessFreeStorageGb)

    val totalCores = driverNode.cpu + (executorNode.cpu * executorsNum)
    val totalMemory = getMemoryWithOverhead(driverNode.memory) + (getMemoryWithOverhead(executorNode.memory) * executorsNum)
    val storagePerExecutor = sparkRuntimeConfigs.executorStorageRequired

    val executorsTotalStorage = math.max(freeStorage, storagePerExecutor) * executorsNum
    val billableStorage = math.max(0, (storagePerExecutor - freeStorage) * executorsNum)

    val runtimeHrs = sparkRuntimeConfigs.runtimeHrs()

    val totalCosts = Map(
      ArchitectureType.ARM64 -> AwsCosts.computeEmrServerlessCosts(runtimeHrs, ArchitectureType.ARM64, totalCores, toGB(totalMemory), toGB(billableStorage), emrServerlessPricing, awsRegion),
      ArchitectureType.X86_64 -> AwsCosts.computeEmrServerlessCosts(runtimeHrs, ArchitectureType.X86_64, totalCores, toGB(totalMemory), toGB(billableStorage), emrServerlessPricing, awsRegion)
    )

    val selectedArchitecture = optType match {
      case TimeOpt => ArchitectureType.X86_64
      case CostOpt | _ => ArchitectureType.ARM64
    }

    val selectedCost = totalCosts(selectedArchitecture)

    val optEnv = EmrServerlessEnv(
      totalCores,
      driverNode.memory + (executorNode.memory * executorsNum),
      executorsTotalStorage + freeStorage,
      selectedArchitecture,
      ContainerRequest(1, driverNode.cpu, driverNode.memory, freeStorage),
      ContainerRequest(executorsNum, executorNode.cpu, executorNode.memory, executorsTotalStorage / executorsNum),
      sparkRuntimeConfigs,
      selectedCost,
      executorNode.cpu / sparkRuntimeConfigs.executorCores
    )

    Some(optEnv)
  }

  private def filterSparkInstances(
    instances: List[EmrInstance],
    request: ResourceRequest,
    optType: OptimalType
  ): List[EmrInstance] = {

    val baseFilters: EmrInstance => Boolean = instance =>
      instance.currentGeneration &&
        !instance.instanceFamily.contains('a') &&
        SparkInstanceFamilies.exists(t => instance.instanceType.startsWith(t)) &&
        instance.memoryGiB >= toGB(request.memory) &&
        instance.vCpu >= request.cores

    optType match {
      case TimeOpt =>
        val timeOptFilters: EmrInstance => Boolean = instance =>
          if (request.storage > byteStringAsBytes(SparkNvmeThreshold))
            instance.networkBandwidthGbps >= 10 &&
              Seq(VolumeType.NVME, VolumeType.SSD).contains(instance.volumeType)
          else
            true

        instances.filter(instance => baseFilters(instance) && timeOptFilters(instance))

      case CostOpt | _ =>
        instances.filter(baseFilters)
    }

  }

  private case class InstanceTest(
    i: EmrInstance,
    totalInstances: Int,
    containersPerInstance: Int,
    costs: EmrCosts,
    wastedCpu: Int,
    wastedMemory: Long
  ) {
    override def toString: String = {
      s"""${i.instanceType} ${costs.total}$$ count:$totalInstances cpi:$containersPerInstance wc:$wastedCpu wm:${humanReadableBytes(wastedMemory)}"""
    }
  }

}
