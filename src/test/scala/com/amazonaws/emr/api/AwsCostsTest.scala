package com.amazonaws.emr.api

import com.amazonaws.emr.api.AwsPricing.{ComputeType, EmrContainersPrice, EmrInstance}
import com.amazonaws.emr.api.AwsPricing.VolumeType.EBS
import com.amazonaws.emr.spark.models.runtime.K8sRequest
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.scalatest.funsuite.AnyFunSuiteLike

class AwsCostsTest extends AnyFunSuiteLike {

  private val MonthRuntimeHours = 720
  private val ebsGbPerMonthCost = 0.08
  private val ebsInstance = EmrInstance(
    "m5.xlarge",
    "m5",
    currentGeneration = true,
    4, 16, EBS, 2, 32, 10, 12288, 0.192, 0.048
  )

  test("testEc2Costs") {
    assert(AwsCosts.ec2Costs(MonthRuntimeHours, ebsInstance.ec2Price, 1, 0) == 138.24)
  }

  test("testEbsCosts") {
    assert(
      AwsCosts.ebsCosts(MonthRuntimeHours, ebsInstance, 1, ebsGbPerMonthCost, byteStringAsBytes("100gb")) == 8.00
    )
  }

  test("testEmrOnEc2UpliftCosts") {
    assert(AwsCosts.emrOnEc2UpliftCosts(MonthRuntimeHours, ebsInstance, 1) == 34.56)
  }

  test("testEmrOnEksUpliftCosts") {
    val req = K8sRequest(1, 100, byteStringAsBytes("300gb"), 0)
    val price = EmrContainersPrice(ComputeType.EC2, 0.00111125, 0.01012)
    assert(AwsCosts.emrOnEksUpliftCosts(MonthRuntimeHours, req, price) == 968.67)
  }

}
