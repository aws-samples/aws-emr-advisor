package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.spark.TestUtils
import com.amazonaws.emr.spark.scheduler.JobOverlapHelper
import com.amazonaws.emr.utils.Formatter.printDurationStr
import org.scalatest.funsuite.AnyFunSuiteLike

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class AppOptimizerAnalyzerTest extends AnyFunSuiteLike with TestUtils {

  // eventLogs
  private val fixedExecAppContext = parse("/job_spark_shell")
  private val sparkPiAppContext = parse("/job_spark_pi")
  private val appAnalyzer = new AppOptimizerAnalyzer


  test("testEstimateRuntime") {
    val fixedExecRealJobTime = JobOverlapHelper.estimatedTimeSpentInJobs(fixedExecAppContext)
    val fixedExecDriverTime = fixedExecAppContext.appInfo.duration - fixedExecRealJobTime

    val simulations = appAnalyzer.estimateRuntime(fixedExecAppContext, 4)
    val runtime2Executors = simulations(2)

    // Opt. number of executors is 2
    assert(simulations(1) > simulations(2))
    // Verify that adding more executors do not change execution time
    simulations.tail.forall(_._2 == simulations(2))

    val jobTime = runtime2Executors - fixedExecDriverTime
    val jobDuration = Duration(jobTime, TimeUnit.MILLISECONDS)

    // This should be 2 minutes processing
    // as we consider the job time when all the tasks can be processed in parallel
    assert(jobDuration.toMinutes == 2)
    assert(jobDuration.toSeconds == 120)

  }

  test("testFindOptimalExecutorCores") {
    val fixedExecRealJobTime = JobOverlapHelper.estimatedTimeSpentInJobs(fixedExecAppContext)
    val fixedExecDriverTime = fixedExecAppContext.appInfo.duration - fixedExecRealJobTime

    val optExecutors4Cores = appAnalyzer.findOptNumExecutors(appAnalyzer.estimateRuntime(fixedExecAppContext, 4))
    println(
      s"Opt. Exec. 4 cores: ${optExecutors4Cores._1} ${printDurationStr(optExecutors4Cores._2 - fixedExecDriverTime)}"
    )
    assert(optExecutors4Cores._1 == 2)

    val optExecutors8Cores = appAnalyzer.findOptNumExecutors(appAnalyzer.estimateRuntime(fixedExecAppContext, 8))
    println(
      s"Opt. Exec. 8 cores: ${optExecutors8Cores._1} ${printDurationStr(optExecutors8Cores._2 - fixedExecDriverTime)}"
    )
    assert(optExecutors8Cores._1 == 1)

    val optExecutors16Cores = appAnalyzer.findOptNumExecutors(appAnalyzer.estimateRuntime(fixedExecAppContext, 16))
    println(
      s"Opt. Exec. 16 cores: ${optExecutors16Cores._1} ${printDurationStr(optExecutors16Cores._2 - fixedExecDriverTime)}"
    )
    assert(optExecutors16Cores._1 == 1)

  }

}
