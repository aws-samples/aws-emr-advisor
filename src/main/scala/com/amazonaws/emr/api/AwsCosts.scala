package com.amazonaws.emr.api

import com.amazonaws.emr.Config.{EmrOnEc2MinStorage, EmrOnEksNodeMinStorage}
import com.amazonaws.emr.api.AwsPricing.ArchitectureType.ArchitectureType
import com.amazonaws.emr.api.AwsPricing.{EmrContainersPrice, EmrInstance, EmrServerlessPrice, VolumeType}
import com.amazonaws.emr.spark.models.runtime.ResourceRequest
import com.amazonaws.emr.utils.Formatter.{asGB, byteStringAsBytes, toGB}
import software.amazon.awssdk.regions.Region

/**
 * AwsCosts provides utilities to estimate the cost of running Spark applications
 * on different AWS environments: EMR on EC2, EMR on EKS, and EMR Serverless.
 *
 * It supports fine-grained calculations of:
 *   - EC2 instance runtime costs (with optional Spot discounts)
 *   - EBS storage usage costs
 *   - EMR service uplifts (per-instance or per-vCPU/memory-hour)
 *   - Aggregated total costs with breakdowns
 *
 * The models returned (`EmrOnEc2Cost`, `EmrOnEksCost`, `EmrServerlessCost`) implement
 * a common `EmrCosts` trait to unify reporting interfaces.
 */
object AwsCosts {

  private val CostsDigits = "6"

  /**
   * Base trait for EMR-related cost breakdowns.
   *
   * @param total        Total cost including EMR service + EC2 + storage.
   * @param emr          Portion attributed to EMR uplift or service fees.
   * @param hardware     Cost of EC2 compute.
   * @param storage      Cost of EBS or ephemeral storage.
   * @param spotDiscount Applied Spot discount (0.0 to 1.0).
   * @param region       AWS region used for pricing.
   */
  sealed trait EmrCosts {
    val total: Double
    val emr: Double
    val hardware: Double
    val storage: Double
    val spotDiscount: Double
    val region: String
  }

  /** Cost model for EMR on EC2 environments. */
  case class EmrOnEc2Cost(
    total: Double,
    emr: Double,
    hardware: Double,
    storage: Double,
    region: String = Region.US_EAST_1.toString,
    spotDiscount: Double = 0.0
  ) extends EmrCosts

  /** Cost model for EMR on EKS environments. */
  case class EmrOnEksCost(
    total: Double,
    emr: Double,
    hardware: Double,
    storage: Double,
    region: String = Region.US_EAST_1.toString,
    spotDiscount: Double = 0.0
  ) extends EmrCosts

  /** Cost model for EMR Serverless environments. */
  case class EmrServerlessCost(
    total: Double,
    cpu: Double,
    memory: Double,
    storage: Double,
    region: String = Region.US_EAST_1.toString,
    spotDiscount: Double = 0.0
  ) extends EmrCosts {
    override val emr: Double = cpu + memory
    override val hardware: Double = 0.0 //
  }

  /**
   * Calculates EC2 instance cost, applying an optional Spot discount.
   *
   * @param runtimeHours   Total runtime in hours.
   * @param ec2Price       On-demand hourly price for the instance.
   * @param numInstances   Number of EC2 instances.
   * @param spotDiscount   Fractional discount (e.g. 0.7 for 30% of original price).
   */
  def ec2Costs(runtimeHours: Double, ec2Price: Double, numInstances: Int, spotDiscount: Double): Double = {
    val total = (1 - spotDiscount) * ec2Price * numInstances * runtimeHours
    s"%.${CostsDigits}f".format(total).toDouble
  }

  /**
   * Estimates EBS storage costs based on allocated storage per instance.
   *
   * @param runtimeHours            Runtime in hours.
   * @param instance                Instance type.
   * @param numInstances            Number of instances.
   * @param ebsPrice                Price per GB-month.
   * @param storagePerInstanceBytes Total provisioned storage in bytes per instance.
   */
  def ebsCosts(
    runtimeHours: Double,
    instance: EmrInstance,
    numInstances: Int,
    ebsPrice: Double,
    storagePerInstanceBytes: Long
  ): Double = {
    if (instance.volumeType.equals(VolumeType.EBS)) {
      val totalStorage = toGB(storagePerInstanceBytes) * numInstances
      s"%.${CostsDigits}f".format(totalStorage * ebsPrice * (runtimeHours * 60 * 60 / (86400 * 30))).toDouble
    } else 0.0
  }

  /**
   * Computes EMR uplift cost for EC2 deployments.
   *
   * @param runtimeHours Runtime in hours.
   * @param instance     Instance type used.
   * @param numInstances Number of EC2 nodes.
   */
  def emrOnEc2UpliftCosts(runtimeHours: Double, instance: EmrInstance, numInstances: Int): Double = {
    if(instance.instanceType == "r8gd.xlarge") {
      println(s"instance.emrPrice: ${instance.emrPrice}")
      println(s"runtimeHours: $runtimeHours")
      println(s"numInstances: $numInstances")
    }
    s"%.${CostsDigits}f".format(instance.emrPrice * runtimeHours * numInstances).toDouble
  }

  /**
   * Computes EMR uplift cost for EKS deployments (vCPU and memory hourly pricing).
   *
   * @param runtimeHours      Runtime in hours.
   * @param request           Resource request specification (per container).
   * @param emrPrice          EMR Containers price info (per vCPU/hr and GB/hr).
   */
  def emrOnEksUpliftCosts(runtimeHours: Double, request: ResourceRequest, emrPrice: EmrContainersPrice): Double = {
    val executorMemGB = asGB(request.memory)
    val cpuCost = emrPrice.CPUHoursPrice * request.cores
    val memoryCost = emrPrice.GBHoursPrice * executorMemGB
    val costs = (memoryCost + cpuCost) * request.count * runtimeHours
    s"%.${CostsDigits}f".format(costs).toDouble
  }

  /**
   * Computes the full cost breakdown for EMR Serverless applications.
   *
   * @param runtimeHrs    Total runtime in hours.
   * @param arch          Target architecture (x86_64 or ARM64).
   * @param totVCpu       Total vCPUs allocated.
   * @param totMemGB      Total memory in GB allocated.
   * @param totStorageGB  Total storage in GB allocated.
   * @param pricing       Serverless pricing definitions by architecture.
   * @param region        AWS region (default: us-east-1).
   */
  def computeEmrServerlessCosts(
    runtimeHrs: Double,
    arch: ArchitectureType,
    totVCpu: Int,
    totMemGB: Int,
    totStorageGB: Int,
    pricing: List[EmrServerlessPrice],
    region: String = Region.US_EAST_1.toString
  ): EmrServerlessCost = {

    val serviceCosts = pricing.filter(_.arch == arch).head
    val cpuCosts = serviceCosts.CPUHoursPrice * totVCpu * runtimeHrs
    val memCosts = serviceCosts.GBHoursPrice * totMemGB * runtimeHrs
    val storageCosts = serviceCosts.storagePrice * totStorageGB * runtimeHrs
    val total = cpuCosts + memCosts + storageCosts

    EmrServerlessCost(
      s"%.${CostsDigits}f".format(total).toDouble,
      s"%.${CostsDigits}f".format(cpuCosts).toDouble,
      s"%.${CostsDigits}f".format(memCosts).toDouble,
      s"%.${CostsDigits}f".format(storageCosts).toDouble,
      region
    )
  }

  /**
   * Computes the full cost breakdown for EMR on EC2 clusters.
   *
   * @param runtimeHrs            Runtime in hours.
   * @param instance              Instance type used.
   * @param numInstances          Total EC2 nodes.
   * @param containersPerInstance Number of YARN containers per node.
   * @param request               YARN resource request per container.
   * @param ebsGbPerMonthCost     EBS cost per GB-month.
   * @param spotDiscount          Spot discount rate (0.0 to 1.0).
   * @param region                AWS region.
   */
  def computeEmrOnEc2Costs(
    runtimeHrs: Double,
    instance: EmrInstance,
    numInstances: Int,
    containersPerInstance: Int,
    request: ResourceRequest,
    ebsGbPerMonthCost: Double,
    spotDiscount: Double = 0.0,
    region: String = Region.US_EAST_1.toString
  ): EmrOnEc2Cost = {

    val emrCosts = AwsCosts.emrOnEc2UpliftCosts(runtimeHrs, instance, numInstances)
    val ec2Costs = AwsCosts.ec2Costs(runtimeHrs, instance.ec2Price, numInstances, spotDiscount)
    val ebsCosts = AwsCosts.ebsCosts(
      runtimeHrs,
      instance,
      request.count,
      ebsGbPerMonthCost,
      request.storage * containersPerInstance + byteStringAsBytes(EmrOnEc2MinStorage)
    )

    val totalCosts = emrCosts + ec2Costs + ebsCosts
    EmrOnEc2Cost(
      s"%.${CostsDigits}f".format(totalCosts).toDouble,
      s"%.${CostsDigits}f".format(emrCosts).toDouble,
      s"%.${CostsDigits}f".format(ec2Costs).toDouble,
      s"%.${CostsDigits}f".format(ebsCosts).toDouble,
      region,
      spotDiscount
    )
  }

  /**
   * Computes the full cost breakdown for EMR on EKS clusters.
   *
   * @param runtimeHrs        Runtime in hours.
   * @param instance          Instance type used for EKS worker nodes.
   * @param numInstances      Total EC2 nodes.
   * @param podsPerInstance   Number of Spark executor pods per node.
   * @param request           Kubernetes resource request per pod.
   * @param ebsGbPerMonthCost EBS cost per GB-month.
   * @param emrContainersPrice EMR uplift per vCPU and GB-hour.
   * @param spotDiscount      Spot discount rate.
   * @param region            AWS region.
   */
  def computeEmrOnEksCosts(
    runtimeHrs: Double,
    instance: EmrInstance,
    numInstances: Int,
    podsPerInstance: Int,
    request: ResourceRequest,
    ebsGbPerMonthCost: Double,
    emrContainersPrice: EmrContainersPrice,
    spotDiscount: Double = 0.0,
    region: String = Region.US_EAST_1.toString
  ): EmrOnEksCost = {

    val ec2Costs = AwsCosts.ec2Costs(runtimeHrs, instance.ec2Price, numInstances, spotDiscount)
    val emrCosts = AwsCosts.emrOnEksUpliftCosts(
      runtimeHrs,
      request,
      emrContainersPrice)
    val ebsCosts = AwsCosts.ebsCosts(
      runtimeHrs,
      instance,
      numInstances,
      ebsGbPerMonthCost,
      request.storage * podsPerInstance + byteStringAsBytes(EmrOnEksNodeMinStorage))

    val totalCosts = emrCosts + ec2Costs + ebsCosts
    EmrOnEksCost(
      s"%.${CostsDigits}f".format(totalCosts).toDouble,
      s"%.${CostsDigits}f".format(emrCosts).toDouble,
      s"%.${CostsDigits}f".format(ec2Costs).toDouble,
      s"%.${CostsDigits}f".format(ebsCosts).toDouble,
      region,
      spotDiscount
    )
  }
}
