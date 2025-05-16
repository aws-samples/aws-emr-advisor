package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.spark.TestUtils
import com.amazonaws.emr.spark.models.{AppContext, AppEfficiency}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar

class SparkBaseOptimizerTest extends AnyFlatSpec with Matchers with MockitoSugar with TestUtils {

  "findOptDriverCores" should "return SparkMaxDriverCores if app is scheduling or result heavy" in {
    val mockAppEfficiency = mock[AppEfficiency]
    when(mockAppEfficiency.isSchedulingHeavy).thenReturn(true)
    when(mockAppEfficiency.isResultHeavy).thenReturn(false)

    val appContext = mock[AppContext]
    when(appContext.appEfficiency).thenReturn(mockAppEfficiency)

    val result = SparkBaseOptimizer.findOptDriverCores(appContext, 50)
    result shouldEqual com.amazonaws.emr.Config.SparkMaxDriverCores
  }

  it should "return computed value within bounds if not scheduling or result heavy" in {
    val mockAppEfficiency = mock[AppEfficiency]
    when(mockAppEfficiency.isSchedulingHeavy).thenReturn(false)
    when(mockAppEfficiency.isResultHeavy).thenReturn(false)

    val appContext = mock[AppContext]
    when(appContext.appEfficiency).thenReturn(mockAppEfficiency)

    val result = SparkBaseOptimizer.findOptDriverCores(appContext, 200)
    result shouldEqual 2
  }

  "getOptimalCoresPerExecutor" should "return cores based on flags" in {
    val mockAppEfficiency = mock[AppEfficiency]
    when(mockAppEfficiency.isTaskShort).thenReturn(true)
    when(mockAppEfficiency.isGcHeavy).thenReturn(false)
    when(mockAppEfficiency.isCpuBound).thenReturn(true)
    when(mockAppEfficiency.isTaskMemoryHeavy).thenReturn(false)

    val appContext = mock[AppContext]
    when(appContext.appEfficiency).thenReturn(mockAppEfficiency)

    val result = SparkBaseOptimizer.findOptCoresPerExecutor(appContext)
    result should contain theSameElementsAs Seq(4, 8, 16)
  }


}
