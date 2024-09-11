package com.amazonaws.emr.spark.scheduler

import com.amazonaws.emr.spark.TestUtils
import com.amazonaws.emr.utils.Formatter.printDurationStr
import org.scalatest.funsuite.AnyFunSuiteLike

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class CompletionEstimatorTest extends AnyFunSuiteLike with TestUtils {

  private val appContext = parse("/job_spark_shell")

  test("testEstimateAppWallClockTimeWithJobLists") {

    val realJobTime = JobOverlapHelper.estimatedTimeSpentInJobs(appContext)
    val realJobDuration = Duration(realJobTime, TimeUnit.MILLISECONDS)
    assert(realJobDuration.toMinutes == 4)
    assert(realJobDuration.toSeconds < (60 * 4 + 4))

    val driverTime = appContext.appInfo.duration - realJobTime

    println(s"Real App Time: ${printDurationStr(appContext.appInfo.duration)}")
    println(s"Real Driver Time: ${printDurationStr(driverTime)}")
    println(s"Real Executor Time: ${printDurationStr(realJobTime)}")

    val appRealDuration = Duration(appContext.appInfo.duration, TimeUnit.MILLISECONDS)
    val appEstimatedTime = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
      appContext,
      executorCount = 1,
      executorsCores = 4,
      appRealDuration.toMillis
    )._1

    val jobEstimatedTime = Duration(appEstimatedTime - driverTime, TimeUnit.MILLISECONDS)

    println(s"Est. App Time: ${printDurationStr(appEstimatedTime)}")
    println(s"Est. Executor Time: ${printDurationStr(jobEstimatedTime.toMillis)}")
    assert(jobEstimatedTime.toMinutes == 4)

    val appEstimatedTime2Exec = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
      appContext,
      executorCount = 2,
      executorsCores = 4,
      appRealDuration.toMillis
    )._1
    val jobEstimatedTime2Exec = Duration(appEstimatedTime2Exec - driverTime, TimeUnit.MILLISECONDS)
    println(s"Est. App Time: ${printDurationStr(appEstimatedTime2Exec)}")
    println(s"Est. Executor Time: ${printDurationStr(jobEstimatedTime2Exec.toMillis)}")
    assert(jobEstimatedTime2Exec.toMinutes == 2)

    val appEstimatedTime2Exec8Cores = CompletionEstimator.estimateAppWallClockTimeWithJobLists(
      appContext,
      executorCount = 2,
      executorsCores = 8,
      appRealDuration.toMillis
    )._1
    val jobEstimatedTime2Exec8Cores = Duration(appEstimatedTime2Exec8Cores - driverTime, TimeUnit.MILLISECONDS)
    println(s"Est. App Time: ${printDurationStr(appEstimatedTime2Exec8Cores)}")
    println(s"Est. Executor Time: ${printDurationStr(jobEstimatedTime2Exec8Cores.toMillis)}")

    // Still 2 minutes with FIFO Scheduler
    assert(jobEstimatedTime2Exec8Cores.toMinutes == 2)

  }

}
