package com.amazonaws.emr.spark.scheduler

import com.amazonaws.emr.spark.TestUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuiteLike

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class JobOverlapHelperTest extends AnyFunSuiteLike with BeforeAndAfter with TestUtils {

  private val appContext = parse("/job_spark_shell")

  test("testEstimatedTimeSpentInJobs") {
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(appContext)
    val jobDuration = Duration(jobTime, TimeUnit.MILLISECONDS)

    // The estimated job time should be close to 4 minutes
    // However we cannot guarantee a perfect time in seconds as this is a simulation,
    // so we add 4 extra seconds which might be acceptable
    assert(jobDuration.toMinutes == 4)
    assert(jobDuration.toSeconds < (60 * 4 + 4))
  }

  test("testMakeJobLists") {
    val jobList = JobOverlapHelper.makeJobLists(appContext)
    assert(jobList.size == 2)
  }

  test("testCriticalPathForAllJobs") {
    val jobTime = JobOverlapHelper.criticalPathForAllJobs(appContext)
    val jobDuration = Duration(jobTime, TimeUnit.MILLISECONDS)

    // This should be 2 minutes processing
    // as we consider the job time when all the tasks can be processed in parallel
    assert(jobDuration.toMinutes == 2)
    assert(jobDuration.toSeconds == 120)
  }

}
