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

    val simulations = appAnalyzer.estimateRuntime(fixedExecAppContext, 4, 4)
    val runtime2Executors = simulations(2)

    // Opt. number of executors is 2
    assert(simulations(1).estimatedAppTimeMs > simulations(2).estimatedAppTimeMs)
    // Verify that adding more executors do not change execution time
    simulations.tail.forall(_._2 == simulations(2))

    val jobTime = runtime2Executors.estimatedAppTimeMs - fixedExecDriverTime
    val jobDuration = Duration(jobTime, TimeUnit.MILLISECONDS)

    // This should be 2 minutes processing
    // as we consider the job time when all the tasks can be processed in parallel
    assert(jobDuration.toMinutes == 2)
    assert(jobDuration.toSeconds == 120)

  }

  test("testFindOptimalExecutorCores") {
    val fixedExecRealJobTime = JobOverlapHelper.estimatedTimeSpentInJobs(fixedExecAppContext)
    val fixedExecDriverTime = fixedExecAppContext.appInfo.duration - fixedExecRealJobTime

    val optExecutors4Cores = appAnalyzer.getOptimumTimeNumExecutors(appAnalyzer.estimateRuntime(fixedExecAppContext, 4, 4)
      .map { s =>
        SimulationWithCores(4, s._1, s._2)
      }.toSeq, 5.0)
    println(
      s"Opt. Exec. 4 cores: ${optExecutors4Cores.executorNum} ${printDurationStr(optExecutors4Cores.appRuntimeEstimate.estimatedAppTimeMs - fixedExecDriverTime)}" +
        s" total core seconds: ${optExecutors4Cores.appRuntimeEstimate.estimatedTotalExecCoreMs/1000}"
    )
    assert(optExecutors4Cores.executorNum == 2)

    val optExecutors8Cores = appAnalyzer.getOptimumTimeNumExecutors(appAnalyzer.estimateRuntime(fixedExecAppContext, 8, 4)
      .map { s =>
        SimulationWithCores(4, s._1, s._2)
      }.toSeq, 5.0)
    println(
      s"Opt. Exec. 8 cores: ${optExecutors8Cores.executorNum} ${printDurationStr(optExecutors8Cores.appRuntimeEstimate.estimatedAppTimeMs - fixedExecDriverTime)}" +
        s" total core seconds: ${optExecutors8Cores.appRuntimeEstimate.estimatedTotalExecCoreMs/1000}"
    )
    assert(optExecutors8Cores.executorNum == 1)

    val optExecutors16Cores = appAnalyzer.getOptimumTimeNumExecutors(appAnalyzer.estimateRuntime(fixedExecAppContext, 16, 4)
      .map { s =>
        SimulationWithCores(4, s._1, s._2)
      }.toSeq, 5.0)
    println(
      s"Opt. Exec. 16 cores: ${optExecutors16Cores.executorNum} ${printDurationStr(optExecutors16Cores.appRuntimeEstimate.estimatedAppTimeMs - fixedExecDriverTime)}" +
        s" total core seconds: ${optExecutors16Cores.appRuntimeEstimate.estimatedTotalExecCoreMs/1000}"
    )
    assert(optExecutors16Cores.executorNum == 1)

  }

  test("findOptCostNumExecutors") {
    val fixedExecRealJobTime = JobOverlapHelper.estimatedTimeSpentInJobs(fixedExecAppContext)
    val fixedExecDriverTime = fixedExecAppContext.appInfo.duration - fixedExecRealJobTime
    
    val coresToResult = Map(
      2 -> (4, 960),
      4 -> (2, 960),
      8 -> (1, 960),
      16 -> (1, 1920))

    coresToResult.foreach { case (coreNum, result) =>

      val simResult = appAnalyzer.estimateRuntime(fixedExecAppContext, coreNum, 500)
        .map { a =>
          SimulationWithCores(coreNum, a._1, a._2)
        }.toSeq
      
      val optExecutorsCores = appAnalyzer.getOptimumCostNumExecutors(simResult, 0.05)
      println(
        s"Opt. Exec. ${coreNum} cores: ${optExecutorsCores.executorNum} executor(s)" +
          s"${printDurationStr(optExecutorsCores.appRuntimeEstimate.estimatedAppTimeMs - fixedExecDriverTime)}, " +
          s"total core seconds: ${optExecutorsCores.appRuntimeEstimate.estimatedTotalExecCoreMs/1000}"
      )
      assert(optExecutorsCores.executorNum == result._1)
      assert(optExecutorsCores.appRuntimeEstimate.estimatedTotalExecCoreMs/1000 == result._2)
    }
  }
  
}
