package com.amazonaws.emr.api

import org.scalatest.funsuite.AnyFunSuiteLike

class AwsEmrTest extends AnyFunSuiteLike {

  test("testEmrReleaseBySparkVersion") {
    assert(AwsEmr.findReleaseBySparkVersion("3.4.0").contains("emr-6.12.0"))
    assert(AwsEmr.findReleaseBySparkVersion("3.4.1").size >= 2)
    assert(AwsEmr.findReleaseBySparkVersion("3.4.1-amzn-0").size >= 2)
    assert(AwsEmr.findReleaseBySparkVersion("3.4.1").contains("emr-6.13.0"))
    assert(AwsEmr.findReleaseBySparkVersion("3.4.1").contains("emr-6.14.0"))
  }

}
