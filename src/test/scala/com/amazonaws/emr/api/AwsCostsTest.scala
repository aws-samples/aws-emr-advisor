package com.amazonaws.emr.api

import com.amazonaws.emr.api.AwsPricing.{ArchitectureType, ComputeType, EmrContainersPrice, EmrInstance, EmrServerlessPrice, VolumeType}
import com.amazonaws.emr.spark.models.runtime.{K8sRequest, ResourceRequest, YarnRequest}
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AwsCostsTest extends AnyFlatSpec with Matchers {

  val testInstance = EmrInstance(
    instanceType = "m5.xlarge",
    instanceFamily = "m5",
    currentGeneration = true,
    vCpu = 4,
    memoryGiB = 16,
    volumeType = VolumeType.EBS,
    volumeNumber = 1,
    volumeSizeGB = 100,
    networkBandwidthGbps = 10,
    yarnMaxMemoryMB = 12000,
    ec2Price = 0.192,
    emrPrice = 0.048
  )

  val testRequest: ResourceRequest = YarnRequest(
    count = 2,
    cores = 2,
    memory = byteStringAsBytes("4g"),
    storage = byteStringAsBytes("20g")
  )

  val runtimeHours = 2.0
  val monthRuntimeHours = 720.0
  val spotDiscount = 0.2
  val ebsPrice = 0.1

  "ec2Costs" should "calculate on-demand compute costs" in {
    val cost = AwsCosts.ec2Costs(runtimeHours, ec2Price = 0.192, numInstances = 10, 0.0)
    cost shouldBe 3.84
  }

  it should "apply spot discount correctly" in {
    val cost = AwsCosts.ec2Costs(runtimeHours, ec2Price = 0.192, numInstances = 10, spotDiscount)
    cost shouldBe 3.072
  }

  "ebsCosts" should "calculate monthly-adjusted storage pricing" in {
    val storageBytes = byteStringAsBytes("2000g")
    val cost = AwsCosts.ebsCosts(12, testInstance, 1, ebsPrice, storageBytes)
    cost shouldBe 3.334 +- 0.001
  }

  "emrOnEc2UpliftCosts" should "multiply instance uplift by count and runtime" in {
    val cost = AwsCosts.emrOnEc2UpliftCosts(runtimeHours, testInstance, 5)
    cost shouldBe 0.48
  }

  "emrOnEksUpliftCosts" should "compute cost from memory and CPU units" in {
    val containerRequest = K8sRequest(1, 100, byteStringAsBytes("300gb"), 0)
    val emrPrice = EmrContainersPrice(compute = ComputeType.EC2, GBHoursPrice = 0.00111125, CPUHoursPrice = 0.01012)
    val cost = AwsCosts.emrOnEksUpliftCosts(runtimeHours, containerRequest, emrPrice)
    cost shouldBe 2.6908 +- 0.0001
  }

  "computeEmrServerlessCosts" should "sum up cpu, memory, and storage prices" in {
    val pricing = List(EmrServerlessPrice(
      arch = ArchitectureType.X86_64,
      CPUHoursPrice = 0.052624,
      GBHoursPrice = 0.0057785,
      storagePrice = 0.01
    ))
    val cost = AwsCosts.computeEmrServerlessCosts(
      runtimeHrs = 1.0,
      arch = ArchitectureType.X86_64,
      totVCpu = 100,
      totMemGB = 750,
      totStorageGB = 0,
      pricing = pricing
    )
    cost.cpu shouldBe 5.2624
    cost.memory shouldBe 4.333875
    cost.total shouldBe 9.596275
  }

}
