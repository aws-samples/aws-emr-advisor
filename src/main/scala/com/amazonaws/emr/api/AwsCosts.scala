package com.amazonaws.emr.api

import com.amazonaws.emr.Config.{EmrOnEc2MinStorage, EmrOnEksNodeMinStorage}
import com.amazonaws.emr.api.AwsPricing.ArchitectureType.ArchitectureType
import com.amazonaws.emr.api.AwsPricing.{EmrContainersPrice, EmrInstance, EmrServerlessPrice, VolumeType}
import com.amazonaws.emr.spark.models.runtime.ResourceRequest
import com.amazonaws.emr.utils.Formatter.{asGB, byteStringAsBytes, toGB}
import software.amazon.awssdk.regions.Region

/*  Helper functions to compute AWS Service Costs */
object AwsCosts {

  private val CostsDigits = "6"

  sealed trait EmrCosts {
    val total: Double
    val emr: Double
    val hardware: Double
    val storage: Double
    val spotDiscount: Double
    val region: String
  }

  case class EmrOnEc2Cost(
    total: Double,
    emr: Double,
    hardware: Double,
    storage: Double,
    region: String = Region.US_EAST_1.toString,
    spotDiscount: Double = 0.0
  ) extends EmrCosts

  case class EmrOnEksCost(
    total: Double,
    emr: Double,
    hardware: Double,
    storage: Double,
    region: String = Region.US_EAST_1.toString,
    spotDiscount: Double = 0.0
  ) extends EmrCosts

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
   * Compute Amazon EC2 costs based on instance type, number of instances, and runtime.
   *
   * @param runtimeHours Instance runtime in hours
   * @param ec2Price     Price of the instance to estimate
   * @param numInstances Number of instances considered
   */
  def ec2Costs(runtimeHours: Double, ec2Price: Double, numInstances: Int, spotDiscount: Double): Double = {
    val total = (1 - spotDiscount) * ec2Price * numInstances * runtimeHours
    s"%.${CostsDigits}f".format(total).toDouble
  }

  /**
   * Compute Amazon EBS costs based on instance type, number of instances, and runtime.
   *
   * @param runtimeHours Instance runtime in hours
   * @param instance     Instance type considered for the computation
   * @param ebsPrice     EBS GB Per Month costs
   * @param numInstances Number of instances considered
   */
  def ebsCosts(
    runtimeHours: Double,
    instance: EmrInstance,
    numInstances: Int,
    ebsPrice: Double,
    storagePerInstanceBytes: Long): Double = {

    if (instance.volumeType.equals(VolumeType.EBS)) {
      val totalStorage = toGB(storagePerInstanceBytes) * numInstances
      s"%.${CostsDigits}f".format(totalStorage * ebsPrice * (runtimeHours * 60 * 60 / (86400 * 30))).toDouble
    } else 0.0

  }

  /**
   * Compute EMR uplift costs for an Amazon EMR on EC2 cluster.
   *
   * @param runtimeHours Runtime in hours of the cluster
   * @param instance     EMR instance type used in the cluster
   * @param numInstances Number of nodes in the cluster
   */
  def emrOnEc2UpliftCosts(runtimeHours: Double, instance: EmrInstance, numInstances: Int): Double = {
    s"%.${CostsDigits}f".format(instance.emrPrice * runtimeHours * numInstances).toDouble
  }

  /**
   * Compute EMR uplift costs for an Amazon on EKS cluster.
   *
   * @param runtimeHours Runtime in hours of the cluster
   * @param request      Executor's pod specifications (memory, cores, number)
   * @param emrPrice     Emr Containers price
   */
  def emrOnEksUpliftCosts(runtimeHours: Double, request: ResourceRequest, emrPrice: EmrContainersPrice): Double = {
    val executorMemGB = asGB(request.memory)
    val cpuCost = emrPrice.CPUHoursPrice * request.cores
    val memoryCost = emrPrice.GBHoursPrice * executorMemGB
    val costs = (memoryCost + cpuCost) * request.count * runtimeHours
    s"%.${CostsDigits}f".format(costs).toDouble
  }

  /**
   * Compute full EMR Serverless costs
   *
   * @param runtimeHrs   Runtime in hours of the application
   * @param arch         Architecture used for the emr application
   * @param totVCpu      Total number of vCores requested
   * @param totMemGB     Total memory requested
   * @param totStorageGB Total storage requested
   * @param region       Aws region
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
   * Compute full EMR on EC2 costs
   *
   * @param runtimeHrs            Runtime in hours of the application
   * @param instance              Ec2 instance type used in the cluster
   * @param numInstances          Number of instances to launch
   * @param containersPerInstance Number of containers that can run on a single node
   * @param request               YARN Resource Request
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
   * Compute full EMR on EKS costs
   *
   * @param runtimeHrs      Runtime in hours of the application
   * @param instance        Ec2 instance type used in the cluster
   * @param numInstances    Number of instances to launch
   * @param podsPerInstance Number of k8s pod that can run on a single node
   * @param request         K8s Resource Request
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
