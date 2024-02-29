package com.amazonaws.emr.spark.models.metrics

import org.apache.spark.executor.ExecutorMetrics

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import scala.util.Try

class AggExecutorMetrics extends AggregateMetrics {

  var count = 0L
  val map = new ConcurrentHashMap[AggExecutorMetrics.Metric, AggregateValue].asScala

  def getMetricMax(metric: AggExecutorMetrics.Metric): Long = Try(map(metric).max).getOrElse(0L)

  private def updateMetric(metric: AggExecutorMetrics.Metric, newValue: Long): Unit = synchronized {

    val aggregateValue = map.getOrElse(metric, new AggregateValue)
    if (count == 0) map(metric) = aggregateValue

    aggregateValue.value += newValue
    aggregateValue.max = math.max(aggregateValue.max, newValue)
    aggregateValue.min = math.min(aggregateValue.min, newValue)
    val delta: Double = newValue - aggregateValue.mean
    aggregateValue.mean += delta / (count + 1)
    aggregateValue.m2 += delta * (newValue - aggregateValue.mean)
    aggregateValue.variance = aggregateValue.m2 / (count + 1)
  }

  def update(tem: ExecutorMetrics): Unit = synchronized {
    updateMetric(AggExecutorMetrics.JVMHeapMemory, tem.getMetricValue("JVMHeapMemory"))
    updateMetric(AggExecutorMetrics.JVMOffHeapMemory, tem.getMetricValue("JVMOffHeapMemory"))
    updateMetric(AggExecutorMetrics.OnHeapExecutionMemory, tem.getMetricValue("OnHeapExecutionMemory"))
    updateMetric(AggExecutorMetrics.OffHeapExecutionMemory, tem.getMetricValue("OffHeapExecutionMemory"))
    updateMetric(AggExecutorMetrics.OnHeapStorageMemory, tem.getMetricValue("OnHeapStorageMemory"))
    updateMetric(AggExecutorMetrics.OffHeapStorageMemory, tem.getMetricValue("OffHeapStorageMemory"))
    updateMetric(AggExecutorMetrics.OnHeapUnifiedMemory, tem.getMetricValue("OnHeapUnifiedMemory"))
    updateMetric(AggExecutorMetrics.OffHeapUnifiedMemory, tem.getMetricValue("OffHeapUnifiedMemory"))
    updateMetric(AggExecutorMetrics.DirectPoolMemory, tem.getMetricValue("DirectPoolMemory"))
    updateMetric(AggExecutorMetrics.MappedPoolMemory, tem.getMetricValue("MappedPoolMemory"))
    updateMetric(AggExecutorMetrics.ProcessTreeJVMVMemory, tem.getMetricValue("ProcessTreeJVMVMemory"))
    updateMetric(AggExecutorMetrics.ProcessTreeJVMRSSMemory, tem.getMetricValue("ProcessTreeJVMRSSMemory"))
    updateMetric(AggExecutorMetrics.ProcessTreePythonVMemory, tem.getMetricValue("ProcessTreePythonVMemory"))
    updateMetric(AggExecutorMetrics.ProcessTreePythonRSSMemory, tem.getMetricValue("ProcessTreePythonRSSMemory"))
    updateMetric(AggExecutorMetrics.ProcessTreeOtherVMemory, tem.getMetricValue("ProcessTreeOtherVMemory"))
    updateMetric(AggExecutorMetrics.ProcessTreeOtherRSSMemory, tem.getMetricValue("ProcessTreeOtherRSSMemory"))
    updateMetric(AggExecutorMetrics.MinorGCCount, tem.getMetricValue("MinorGCCount"))
    updateMetric(AggExecutorMetrics.MinorGCTime, tem.getMetricValue("MinorGCTime"))
    updateMetric(AggExecutorMetrics.MajorGCCount, tem.getMetricValue("MajorGCCount"))
    updateMetric(AggExecutorMetrics.MajorGCTime, tem.getMetricValue("MajorGCTime"))
    updateMetric(AggExecutorMetrics.TotalGCTime, tem.getMetricValue("TotalGCTime"))
    count += 1
  }

}

object AggExecutorMetrics extends Enumeration {

  type Metric = Value
  val JVMHeapMemory,
  JVMOffHeapMemory,
  OnHeapExecutionMemory,
  OffHeapExecutionMemory,
  OnHeapStorageMemory,
  OffHeapStorageMemory,
  OnHeapUnifiedMemory,
  OffHeapUnifiedMemory,
  DirectPoolMemory,
  MappedPoolMemory,
  ProcessTreeJVMVMemory,
  ProcessTreeJVMRSSMemory,
  ProcessTreePythonVMemory,
  ProcessTreePythonRSSMemory,
  ProcessTreeOtherVMemory,
  ProcessTreeOtherRSSMemory,
  MinorGCCount,
  MinorGCTime,
  MajorGCCount,
  MajorGCTime,
  TotalGCTime
  = Value

}