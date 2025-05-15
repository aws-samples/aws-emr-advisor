package org.apache.spark.utils

import org.scalatest.funsuite.AnyFunSuiteLike

class SparkSubmitHelperTest extends AnyFunSuiteLike {

  test("testEmrServerlessPython") {

    val submitCmd = Seq(
      "org.apache.spark.deploy.SparkSubmit",
      "--deploy-mode client",
      "--conf spark.emr-serverless.driver.disk=20G",
      "--conf spark.emr-serverless.lakeformation.enabled=",
      "s3://testbucket/script.py",
      "arg1",
      "arg2"
    ).mkString(" ")

    val parsed = SparkSubmitHelper.parse(submitCmd)

    assert(parsed.appScriptJarPath == "s3://testbucket/script.py")
    assert(parsed.appArguments.size == 2)
    assert(parsed.deployMode.contains("client"))
    assert(parsed.isPython)

  }

}
