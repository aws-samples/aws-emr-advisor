package com.amazonaws.emr.api

import com.amazonaws.emr.api.AwsEmr.findReleaseBySparkVersion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AwsEmrTest extends AnyFlatSpec with Matchers {

  "findReleaseBySparkVersion" should "use cached results" in {
    val release = findReleaseBySparkVersion("3.4.0")
    release shouldBe List("emr-6.12.0")
  }

  it should "find multiple emr releases" in {
    val releases = findReleaseBySparkVersion("3.4.1")
    releases.size shouldBe 3
  }

}
