package com.amazonaws.emr.api

import com.amazonaws.emr.api.AwsPricing.getEmrAvailableInstances
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AwsPricingTest extends AnyFlatSpec with Matchers {

  val fixedPricing = Map(
    "r8gd.xlarge" -> (0.29392, 0.07348),
    "r7g.8xlarge" -> (1.7136, 0.4284),
    "m5.xlarge" -> (0.192, 0.048),
  )

  "getEmrAvailableInstances" should "retrieve correct ec2 and emr pricing" in {
    val instanceMap = getEmrAvailableInstances().map(i => i.instanceType -> (i.ec2Price, i.emrPrice)).toMap
    instanceMap("r8gd.xlarge")._1 shouldBe fixedPricing("r8gd.xlarge")._1
    instanceMap("r8gd.xlarge")._2 shouldBe fixedPricing("r8gd.xlarge")._2
    instanceMap("r7g.8xlarge")._1 shouldBe fixedPricing("r7g.8xlarge")._1
    instanceMap("r7g.8xlarge")._2 shouldBe fixedPricing("r7g.8xlarge")._2
    instanceMap("m5.xlarge")._1 shouldBe fixedPricing("m5.xlarge")._1
    instanceMap("m5.xlarge")._2 shouldBe fixedPricing("m5.xlarge")._2
  }

}
