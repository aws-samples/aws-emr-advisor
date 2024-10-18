package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, EmrInstance, ComputeType}
import com.amazonaws.emr.utils.Formatter.{toGB, toMB, humanReadableBytes, roundUp, byteStringAsBytes}
import com.amazonaws.emr.Config._
import com.amazonaws.emr.api.{AwsPricing, AwsCosts}
import com.amazonaws.emr.spark.models.runtime.{Environment, EmrOnEksEnv, EmrServerlessEnv, EmrEnvironment, EmrOnEc2Env, SparkRuntime, ResourceRequest, ContainerRequest}
import com.amazonaws.emr.api.AwsCosts.{EmrCosts, EmrOnEc2Cost, EmrOnEksCost}
import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.findWorkerByCpuMemory
import com.amazonaws.emr.spark.models.runtime.Environment._
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.OptimalTypes._
import com.amazonaws.emr.spark.models.runtime.SparkRuntime.getMemoryWithOverhead
import com.amazonaws.emr.utils.Constants.{ParamSpot, ParamRegion}
import org.apache.spark.internal.Logging

class AppEnvCostAnalyzer extends AppAnalyzer with Logging {

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logInfo("Analyze costs...")

    // Check for any ec2 spot discount to apply
    val spotDiscount: Double = options.getOrElse(ParamSpot.name, "0.0").toDouble

    // check for specific regions to compute costs
    val awsRegion = options.getOrElse(ParamRegion.name, "us-east-1")
    logInfo(s"Computing costs for $awsRegion region")

    // get instances with corresponding price
    val allInstances = AwsPricing
      .getEmrAvailableInstances(awsRegion)
      .filter(_.vCpu >= 2)
      .filter(_.memoryGiB >= 8)

    val emrOnEksUplift = AwsPricing.getEmrContainersPrice(awsRegion)
      .filter(_.compute.equals(ComputeType.EC2))
      .head

    val ebsGbPerMonthCost = AwsPricing.getEbsServicePrice(awsRegion)
      .filter(_.volumeType == EbsDefaultStorage)
      .head
      .price

    Seq(TimeOpt, CostOpt, TimeCapped).foreach{
      findOptimizedEnvForAllDeployment
    }
    
    def findOptimizedEnvForAllDeployment(optimalType: OptimalType): Unit = {
      val sparkConf = appContext.appRecommendations.sparkConfs.getOrElse(optimalType, SparkRuntime.empty)
      val driverReq = ContainerRequest(1, sparkConf.driverCores, sparkConf.driverMemory, 0L)
      val executorReq = ContainerRequest(
        sparkConf.executorsNum,
        sparkConf.executorCores,
        sparkConf.executorMemory,
        sparkConf.executorStorageRequired
      )

      findOptEnv(allInstances, sparkConf, driverReq, executorReq, EC2)
        .map(appContext.appRecommendations.emrOnEc2Envs.put(optimalType, _))
      findOptEnv(allInstances, sparkConf, driverReq, executorReq, EKS)
        .map(appContext.appRecommendations.emrOnEksEnvs.put(optimalType, _))
      findEmrServerlessOptimal(sparkConf)
        .map(appContext.appRecommendations.emrServerlessEnvs.put(optimalType, _))
    }

    
    // ========================================================================
    // EMR EC2 / EKS
    // ========================================================================
    def evaluate(
      i: EmrInstance,
      runtimeConfig: SparkRuntime,
      request: ResourceRequest,
      ebsGbPerMonthCost: Double,
      deployment: name
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
      //println(s"""${i.instanceType} - ${i.vCpu} - ${request.cores}""")
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

    def findOptEnv(
      instances: List[EmrInstance],
      sparkRuntimeConfigs: SparkRuntime,
      driver: ResourceRequest,
      executors: ResourceRequest,
      deployment: name
    ): Option[EmrEnvironment] = {

      // evaluate instances for spark executors
      val workerNodes = {
        deployment match {
          case EC2 =>
            logInfo(s"Compute EC2 Exec - ${executors}")
            filterSparkInstances(instances, executors)
              .map(i => evaluate(i, sparkRuntimeConfigs, executors, ebsGbPerMonthCost, EC2))
              .filter(_.containersPerInstance >= 1)
              .filter(s => s.totalInstances >= EmrOnEc2ClusterMinNodes)
              .filter(_.containersPerInstance <= EmrOnEc2MaxContainersPerInstance)

          case EKS =>
            logInfo(s"Compute EKS Exec - ${executors}")
            filterSparkInstances(instances, executors)
              .map(i => evaluate(i, sparkRuntimeConfigs, executors, ebsGbPerMonthCost, Environment.EKS))
              .filter(_.containersPerInstance >= 1)
              .filter(_.containersPerInstance <= EmrOnEksMaxPodsPerInstance)
          case _ => Nil
        }
      }.sortBy(_.costs.total).take(40)

      //workerNodes.foreach(println)

      val workerNode: InstanceTest = workerNodes.minBy(_.costs.total)
      val workersInstanceGen = workerNode.i.instanceFamily.split("\\D+").find(_.nonEmpty).getOrElse("0")

      // evaluate instances for spark driver
      val driverNodeTmp = {
        deployment match {
          case EC2 =>
            logInfo(s"Compute EC2 Driver - ${driver}")
            filterSparkInstances(instances, driver)
              .filter(_.instanceFamily.contains(workersInstanceGen))
              .map(i => evaluate(i, sparkRuntimeConfigs, driver, ebsGbPerMonthCost, EC2))
              .filter(_.containersPerInstance >= 1)
          case EKS =>
            logInfo(s"Compute EKS Driver - ${driver}")
            filterSparkInstances(instances, driver)
              .filter(_.instanceFamily.contains(workersInstanceGen))
              .map(i => evaluate(i, sparkRuntimeConfigs, driver, ebsGbPerMonthCost, Environment.EKS))
              .filter(_.containersPerInstance >= 1)
          case _ => Nil
        }
      }.sortBy(_.costs.total).take(40)

      //driverNodeTmp.foreach(println)
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

    // ========================================================================
    // EMR Serverless
    // ========================================================================
    def findEmrServerlessOptimal(sparkRuntimeConfigs: SparkRuntime): Option[EmrEnvironment] = {

      val driverNode = findWorkerByCpuMemory(sparkRuntimeConfigs.driverCores, sparkRuntimeConfigs.driverMemory)
      val executorNode = findWorkerByCpuMemory(sparkRuntimeConfigs.executorCores, sparkRuntimeConfigs.executorMemory)
      val executorsNum = sparkRuntimeConfigs.executorsNum

      val freeStorage = byteStringAsBytes(EmrServerlessFreeStorageGb)

      val totalCores = driverNode.cpu + executorNode.cpu * executorsNum
      val totalMemory = getMemoryWithOverhead(driverNode.memory) + getMemoryWithOverhead(executorNode.memory) * executorsNum
      val storagePerExecutor = sparkRuntimeConfigs.executorStorageRequired

      val executorsTotalStorage = if (freeStorage > storagePerExecutor) freeStorage * executorsNum else storagePerExecutor * executorsNum
      val billableStorage = if (freeStorage > storagePerExecutor) 0 else (storagePerExecutor - freeStorage) * executorsNum

      val runtimeHrs = sparkRuntimeConfigs.runtimeHrs()

      val totalCostsArm = AwsCosts.computeEmrServerlessCosts(runtimeHrs, ArchitectureType.ARM64, totalCores, toGB(totalMemory), toGB(billableStorage), awsRegion)
      val totalCostsX86 = AwsCosts.computeEmrServerlessCosts(runtimeHrs, ArchitectureType.X86_64, totalCores, toGB(totalMemory), toGB(billableStorage), awsRegion)
      
      if (totalCostsArm.total <= totalCostsX86.total)
        Some(EmrServerlessEnv(
          totalCores,
          driverNode.memory + (executorNode.memory * executorsNum),
          executorsTotalStorage + freeStorage,
          ArchitectureType.ARM64,
          ContainerRequest(1, driverNode.cpu, driverNode.memory, freeStorage),
          ContainerRequest(executorsNum, executorNode.cpu, executorNode.memory, executorsTotalStorage / executorsNum),
          totalCostsArm,
          executorNode.cpu/sparkRuntimeConfigs.executorCores
        ))
      else
        Some(EmrServerlessEnv(
          totalCores,
          driverNode.memory + (executorNode.memory * executorsNum),
          executorsTotalStorage + freeStorage,
          ArchitectureType.X86_64,
          ContainerRequest(1, driverNode.cpu, driverNode.memory, freeStorage),
          ContainerRequest(executorsNum, executorNode.cpu, executorNode.memory, executorsTotalStorage / executorsNum),
          totalCostsX86,
          executorNode.cpu/sparkRuntimeConfigs.executorCores
        ))

    }

    case class InstanceTest(
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
  private def filterSparkInstances(instances: List[EmrInstance], request: ResourceRequest): List[EmrInstance] = {
    instances
      .filter(_.currentGeneration == true)
      .filter(!_.instanceFamily.contains('a'))
      .filter(i => SparkInstanceFamilies.exists(t => i.instanceType.startsWith(t)))
      .filter(_.memoryGiB >= toGB(request.memory))
      .filter(_.networkBandwidthGbps >= 10)
      .filter(_.vCpu >= request.cores)
  }

}