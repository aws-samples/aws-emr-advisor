package com.amazonaws.emr.spark.optimizer

import com.amazonaws.emr.spark.models.timespan.StageSummaryMetrics
import com.amazonaws.emr.spark.models.{AppContext, AppEfficiency}
import org.mockito.MockitoSugar.{mock, when}

object TestOptimizerContext {

  def empty: AppContext = new AppContext(
    null, null, null, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty
  )

  def mockDriverCoresAppContext(isSchedulingHeavy: Boolean, isResultHeavy: Boolean): AppContext = {
    val mockAppEfficiency = mock[AppEfficiency]
    when(mockAppEfficiency.isSchedulingHeavy).thenReturn(isSchedulingHeavy)
    when(mockAppEfficiency.isResultHeavy).thenReturn(isResultHeavy)
    val appContext = mock[AppContext]
    when(appContext.appEfficiency).thenReturn(mockAppEfficiency)
    appContext
  }

  def mockExecutorCoresAppContext(
    isTaskShort: Boolean,
    isGcHeavy: Boolean,
    isCpuBound:Boolean,
    isTaskMemoryHeavy: Boolean
  ): AppContext = {
    val mockAppEfficiency = mock[AppEfficiency]
    when(mockAppEfficiency.isTaskShort).thenReturn(isTaskShort)
    when(mockAppEfficiency.isGcHeavy).thenReturn(isGcHeavy)
    when(mockAppEfficiency.isCpuBound).thenReturn(isCpuBound)
    when(mockAppEfficiency.isTaskMemoryHeavy).thenReturn(isTaskMemoryHeavy)
    val appContext = mock[AppContext]
    when(appContext.appEfficiency).thenReturn(mockAppEfficiency)
    appContext
  }

  def createMockAppContext(
    stageMetrics: List[StageSummaryMetrics]
  ): AppContext = {
    val mockAppEfficiency = mock[AppEfficiency]
    when(mockAppEfficiency.stageSummaryMetrics).thenReturn(stageMetrics)
    val appContext = mock[AppContext]
    when(appContext.appEfficiency).thenReturn(mockAppEfficiency)
    appContext
  }

}
