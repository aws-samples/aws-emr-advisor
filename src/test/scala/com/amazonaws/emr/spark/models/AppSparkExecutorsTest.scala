package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.TestUtils
import org.scalatest.funsuite.AnyFunSuiteLike

class AppSparkExecutorsTest extends AnyFunSuiteLike with TestUtils {

  private val appContext = parse("/job_spark_shell")

  test("testExecutorsTotalCores") {
    assert(appContext.appSparkExecutors.executorsTotalCores == 4)
  }

  test("testExecutorsLaunched") {
    assert(appContext.appSparkExecutors.executorsLaunched == 1)
  }

  test("testGetMaxJvmMemoryUsed") {
    assert(appContext.appSparkExecutors.getMaxJvmMemoryUsed == 227435608)
  }

  test("testIsComputeIntensive") {
    assert(appContext.appSparkExecutors.isComputeIntensive)
  }

  test("testGetMaxTaskMemoryUsed") {
    assert(appContext.appSparkExecutors.getMaxTaskMemoryUsed == 0)
  }

  test("testIsShuffleWriteUniform") {
    assert(appContext.appSparkExecutors.isShuffleWriteUniform)
  }

  test("testGetTotalDiskBytesSpilled") {
    assert(appContext.appSparkExecutors.getTotalDiskBytesSpilled == 0)
  }

  test("testGetRequiredStoragePerExecutor") {
    assert(appContext.appSparkExecutors.getMaxDiskBytesSpilled == 0)
  }

  test("testIsMemoryIntensive") {
    assert(!appContext.appSparkExecutors.isMemoryIntensive)
  }

  test("testExecutorsMaxRunning") {
    assert(appContext.appSparkExecutors.executorsMaxRunning == 1)
  }

  test("testGetTotalTasksProcessed") {
    assert(appContext.appSparkExecutors.getTotalTasksProcessed == 16)
  }

}
