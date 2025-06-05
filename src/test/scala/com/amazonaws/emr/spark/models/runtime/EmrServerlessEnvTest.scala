package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.{isValidEmrServerlessRuntime, normalizeEmrServerlessRuntime}
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.scalatest.flatspec.AnyFlatSpec

class EmrServerlessEnvTest extends AnyFlatSpec {

  val mem1Gb = byteStringAsBytes("1g")
  val mem2Gb = byteStringAsBytes("2g")
  val mem4Gb = byteStringAsBytes("4g")
  val mem17Gb = byteStringAsBytes("17g")
  val mem40Gb = byteStringAsBytes("40g")
  val mem60Gb = byteStringAsBytes("60g")
  val mem94Gb = byteStringAsBytes("94g")
  val mem96Gb = byteStringAsBytes("96g")
  val mem320Gb = byteStringAsBytes("320g")

  // Correct
  val t1 = SparkRuntime(100L, 1, mem2Gb, 1, mem2Gb, 0L, 1, Map.empty)
  val t2 = SparkRuntime(100L, 1, mem2Gb, 8, mem40Gb, 0L, 1, Map.empty)
  val t3 = SparkRuntime(100L, 1, mem2Gb, 8, mem60Gb, 0L, 1, Map.empty)

  // Incorrect
  val t4 = SparkRuntime(100L, 1, mem1Gb, 1, mem1Gb, 0L, 1, Map.empty)
  val t5 = SparkRuntime(100L, 1, mem2Gb, 1, mem1Gb, 0L, 1, Map.empty)
  val t6 = SparkRuntime(100L, 1, mem2Gb, 8, mem17Gb, 0L, 1, Map.empty)
  val t7 = SparkRuntime(100L, 1, mem2Gb, 16, mem320Gb, 0L, 1, Map.empty)
  val t8 = SparkRuntime(100L, 3, mem4Gb, 1, mem1Gb, 0L, 1, Map.empty)
  val t9 = SparkRuntime(100L, 1, mem2Gb, 8, mem1Gb, 0L, 1, Map.empty)
  val t10 = SparkRuntime(100L, 1, mem2Gb, 9, mem60Gb, 0L, 1, Map.empty)
  val t11 = SparkRuntime(100L, 1, mem2Gb, 9, mem94Gb, 0L, 1, Map.empty)

  "isValidEmrServerlessRuntime" should "detect valid EMR Serverless runtimes" in {
    assert(isValidEmrServerlessRuntime(t1))
    assert(isValidEmrServerlessRuntime(t2))
    assert(isValidEmrServerlessRuntime(t3))
  }

  it should "detect invalid EMR Serverless runtimes" in {
    assert(!isValidEmrServerlessRuntime(t4))
    assert(!isValidEmrServerlessRuntime(t5))
    assert(!isValidEmrServerlessRuntime(t6))
    assert(!isValidEmrServerlessRuntime(t7))
    assert(!isValidEmrServerlessRuntime(t8))
    assert(!isValidEmrServerlessRuntime(t9))
    assert(!isValidEmrServerlessRuntime(t10))
    assert(!isValidEmrServerlessRuntime(t11))
  }

  "normalizeEmrServerlessRuntime" should "avoid modifications in valid runtimes" in {
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t1)))
    assert(normalizeEmrServerlessRuntime(t1) == t1)
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t2)))
    assert(normalizeEmrServerlessRuntime(t2) == t2)
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t3)))
    assert(normalizeEmrServerlessRuntime(t3) == t3)
  }

  it should "adjust runtimes for invalid worker configurations" in {
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t4)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t5)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t6)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t7)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t8)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t9)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t10)))
    assert(isValidEmrServerlessRuntime(normalizeEmrServerlessRuntime(t11)))
    assert(normalizeEmrServerlessRuntime(t11).executorMemory == mem96Gb)
  }

}
