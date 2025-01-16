package com.amazonaws.emr.utils

import com.amazonaws.emr.utils.Constants.{SparkAppOptParams, SparkAppParams}
import org.scalatest.funsuite.AnyFunSuiteLike

class ArgParserTest extends AnyFunSuiteLike {

  private val testArgs = List(
    "--bucket", "spark.test.bucket",
    "./src/test/resources/job_spark_pi"
  )

  test("testParseSparkCmd") {

    val className = this.getClass.getCanonicalName.replace("$","")
    val baseCmd = s"spark-submit --class $className"

    val argParser = new ArgParser(baseCmd, 1, SparkAppParams, SparkAppOptParams)
    val argMap = argParser.parse(testArgs)

    assert(argMap.size == 2)
    assert(argMap("bucket") == "spark.test.bucket")
    assert(argMap("filename") == "./src/test/resources/job_spark_pi")
  }

}
