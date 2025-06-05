package com.amazonaws.emr.spark.models

import com.amazonaws.emr.spark.TestUtils
import org.scalatest.flatspec.AnyFlatSpec

class AppSparkExecutorsTest extends AnyFlatSpec with TestUtils {

  private val appContext = parse("/job_spark_shell")

  "Spark Executor Analysis" should "detect correct cores" in {
    assert(appContext.appSparkExecutors.executorsTotalCores == 4)
  }

  it should "detect total number of executors launched" in {
    assert(appContext.appSparkExecutors.executorsLaunched == 1)
  }

  it should "detect max amount of JVM Memory used" in {
    assert(appContext.appSparkExecutors.getMaxJvmMemoryUsed == 227435608)
  }

  it should "detect max amount of Memory used in a task" in {
    assert(appContext.appSparkExecutors.getMaxTaskMemoryUsed == 0)
  }

  it should "detect if an application is Compute intensive" in {
    assert(appContext.appSparkExecutors.isComputeIntensive)
  }

  it should "detect if shuffle data written is uniform across executors" in {
    assert(appContext.appSparkExecutors.isShuffleWriteUniform)
  }

  it should "detect if the amount of memory spilled to disks" in {
    assert(appContext.appSparkExecutors.getTotalDiskBytesSpilled == 0)
  }

  it should "detect if the max amount of memory spilled to disks" in {
    assert(appContext.appSparkExecutors.getMaxDiskBytesSpilled == 0)
  }

  it should "detect the max amount of executors running concurrently" in {
    assert(appContext.appSparkExecutors.executorsMaxRunning == 1)
  }

  it should "detect the max total number of tasks processed" in {
    assert(appContext.appSparkExecutors.getTotalTasksProcessed == 16)
  }

}
