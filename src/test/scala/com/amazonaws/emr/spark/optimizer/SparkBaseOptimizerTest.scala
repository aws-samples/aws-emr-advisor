package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.Config.{SparkExecutorCoresBalanced, SparkExecutorCoresCpuIntensive, SparkExecutorCoresMemoryIntensive}
import com.amazonaws.emr.spark.TestUtils
import com.amazonaws.emr.spark.analyzer.{AppRuntimeEstimate, SimulationWithCores}
import com.amazonaws.emr.spark.models.timespan.StageSummaryMetrics
import com.amazonaws.emr.spark.optimizer.TestOptimizerContext._
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkBaseOptimizerTest extends AnyFlatSpec with Matchers with MockitoSugar with TestUtils {

  "recommendDriverCores" should "return SparkMaxDriverCores if app is scheduling or result heavy" in {
    val appContext = mockDriverCoresAppContext(isSchedulingHeavy = true, isResultHeavy = false)
    val sparkBaseOptimizer = new SparkBaseOptimizer(appContext)
    val result = sparkBaseOptimizer.recommendDriverCores(50)
    result shouldEqual com.amazonaws.emr.Config.SparkMaxDriverCores
  }

  it should "return computed value within bounds if not scheduling or result heavy" in {
    val appContext = mockDriverCoresAppContext(isSchedulingHeavy = false, isResultHeavy = false)
    val sparkBaseOptimizer = new SparkBaseOptimizer(appContext)
    val result = sparkBaseOptimizer.recommendDriverCores(200)
    result shouldEqual 2
  }

  "recommendExecutorCores" should "return balanced cores sets when unsure" in {
    val appContext = mockExecutorCoresAppContext(
      isTaskShort = false, isGcHeavy = false, isCpuBound = false, isTaskMemoryHeavy = false
    )
    val sparkBaseOptimizer = new SparkBaseOptimizer(appContext)
    val result = sparkBaseOptimizer.recommendExecutorCores()
    result should contain theSameElementsAs SparkExecutorCoresBalanced
  }

  it should "return smaller core sets when task are memory intensive" in {
    val appContext = mockExecutorCoresAppContext(
      isTaskShort = true, isGcHeavy = false, isCpuBound = true, isTaskMemoryHeavy = true
    )
    val sparkBaseOptimizer = new SparkBaseOptimizer(appContext)
    val result = sparkBaseOptimizer.recommendExecutorCores()
    result should contain theSameElementsAs SparkExecutorCoresMemoryIntensive
  }

  it should "return greater core sets when task are short" in {
    val appContext = mockExecutorCoresAppContext(
      isTaskShort = true, isGcHeavy = false, isCpuBound = true, isTaskMemoryHeavy = false
    )
    val sparkBaseOptimizer = new SparkBaseOptimizer(appContext)
    val result = sparkBaseOptimizer.recommendExecutorCores()
    result should contain theSameElementsAs SparkExecutorCoresCpuIntensive
  }

  "recommendExecutorNumber" should "pick the config before diminishing returns" in {
    val simulations = Seq(
      SimulationWithCores(coresPerExecutor = 4, executorNum = 2, AppRuntimeEstimate(10000, 80000)),
      SimulationWithCores(coresPerExecutor = 4, executorNum = 4, AppRuntimeEstimate(8000, 64000)),
      SimulationWithCores(coresPerExecutor = 4, executorNum = 8, AppRuntimeEstimate(7000, 56000)),
      SimulationWithCores(coresPerExecutor = 4, executorNum = 16, AppRuntimeEstimate(6800, 54400))
    )

    val sparkBaseOptimizer = new SparkBaseOptimizer(TestOptimizerContext.empty)
    val result = sparkBaseOptimizer.recommendExecutorNumber(simulations, maxDrop = 10.0)

    result.executorNum shouldBe 8
  }

  behavior of "recommendExecutorMemory"

  it should "calculate executor memory for low memory workload" in {
    val stageMetrics = List(
      StageSummaryMetrics(
        id = 0,
        numberOfTasks = 1000L,
        stageAvgPeakMemory = 500L * 1024 * 1024,
        stageAvgMemorySpilled = 0L,
        stageMaxPeakMemory = 500L * 1024 * 1024,
        stageTotalMemorySpilled = 0L)
    )
    val mockContext = createMockAppContext(stageMetrics)
    val sparkBaseOptimizer = new SparkBaseOptimizer(mockContext)
    sparkBaseOptimizer.recommendExecutorMemory(2, 4) shouldBe byteStringAsBytes("2g")
  }

  it should "calculate executor memory using task concurrency and avg peak memory" in {
    val stageMetrics = List(
      StageSummaryMetrics(
        id = 0,
        numberOfTasks = 10L,
        stageAvgPeakMemory = 5L * 1024 * 1024 * 1024,
        stageAvgMemorySpilled = 0L,
        stageMaxPeakMemory = 5L * 1024 * 1024 * 1024,
        stageTotalMemorySpilled = 0L)
    )
    val mockContext = createMockAppContext(stageMetrics)
    val sparkBaseOptimizer = new SparkBaseOptimizer(mockContext)
    sparkBaseOptimizer.recommendExecutorMemory(2, 10) shouldBe byteStringAsBytes("11g")
  }

  it should "estimate executor memory requirements, ignoring spill data when it's negligible" in {
    val stageMetrics = List(
      StageSummaryMetrics(
        id = 0,
        numberOfTasks = 10L,
        stageAvgPeakMemory = 5L * 1024 * 1024 * 1024,
        stageAvgMemorySpilled = 1L * 1024 * 1024 * 1024,
        stageMaxPeakMemory = 5L * 1024 * 1024 * 1024,
        stageTotalMemorySpilled = 10L * 1024 * 1024 * 1024)
    )
    val mockContext = createMockAppContext(stageMetrics)
    val sparkBaseOptimizer = new SparkBaseOptimizer(mockContext)
    sparkBaseOptimizer.recommendExecutorMemory(2, 10) shouldBe byteStringAsBytes("11g")
  }

  it should "estimate executor memory requirements, compensating heavy data spill" in {
    val stageMetrics = List(
      StageSummaryMetrics(
        id = 0,
        numberOfTasks = 50000L,
        stageAvgPeakMemory = 5L * 1024 * 1024 * 1024,
        stageAvgMemorySpilled = 10L * 1024 * 1024 * 1024,
        stageMaxPeakMemory = 5L * 1024 * 1024 * 1024,
        stageTotalMemorySpilled = 500L * 1024 * 1024 * 1024)
    )
    val mockContext = createMockAppContext(stageMetrics)
    val sparkBaseOptimizer = new SparkBaseOptimizer(mockContext)
    sparkBaseOptimizer.recommendExecutorMemory(2, 10) shouldBe byteStringAsBytes("15g")
  }

}
