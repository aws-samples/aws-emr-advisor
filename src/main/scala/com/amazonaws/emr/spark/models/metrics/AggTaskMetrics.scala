package com.amazonaws.emr.spark.models.metrics

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable
import scala.util.Try

class AggTaskMetrics() extends AggregateMetrics {

  var count = 0L

  var completedTasks = 0L
  var failedTasks = 0L
  var killedTasks = 0L

  val map = new mutable.HashMap[AggTaskMetrics.Metric, AggregateValue]()

  private def updateMetric(metric: AggTaskMetrics.Metric, newValue: Long): Unit = synchronized {
    val aggregateValue = map.getOrElse(metric, new AggregateValue)
    if (count == 0) {
      map(metric) = aggregateValue
    }

    aggregateValue.samples += 1
    aggregateValue.value += newValue
    aggregateValue.max = math.max(aggregateValue.max, newValue)
    aggregateValue.min = math.min(aggregateValue.min, newValue)
    val delta: Double = newValue - aggregateValue.mean
    aggregateValue.mean += delta / (count + 1)
    aggregateValue.m2 += delta * (newValue - aggregateValue.mean)
    aggregateValue.variance = aggregateValue.m2 / (count + 1)
  }

  def update(tm: TaskMetrics, ti: TaskInfo): Unit = synchronized {

    // executor
    // tm.executorCpuTime - CPU Time the executor spends actually running the task (including fetching shuffle data) in nanoseconds.
    // tm.executorRunTime - Time the executor spends actually running the task (including fetching shuffle data)
    // tm.executorDeserializeTime - Time taken on the executor to deserialize this task
    // tm.executorDeserializeCpuTime - CPU Time taken on the executor to deserialize this task in nanoseconds
    updateMetric(AggTaskMetrics.executorDeserializeCpuTime, tm.executorDeserializeCpuTime)
    updateMetric(AggTaskMetrics.executorDeserializeTime, tm.executorDeserializeTime)
    updateMetric(AggTaskMetrics.executorCpuTime, tm.executorCpuTime)
    updateMetric(AggTaskMetrics.executorRunTime, tm.executorRunTime)

    // other metrics
    // tm.jvmGCTime - Amount of time the JVM spent in garbage collection while executing this task
    // tm.resultSize - The number of bytes this task transmitted back to the driver as the TaskResult
    // tm.resultSerializationTime - Amount of time spent serializing the task result
    updateMetric(AggTaskMetrics.jvmGCTime, tm.jvmGCTime)
    updateMetric(AggTaskMetrics.resultSize, tm.resultSize)
    updateMetric(AggTaskMetrics.resultSerializationTime, tm.resultSerializationTime)

    // spill metrics
    // tm.diskBytesSpilled - The number of on-disk bytes spilled by this task
    // tm.memoryBytesSpilled - The number of in-memory bytes spilled by this task
    updateMetric(AggTaskMetrics.diskBytesSpilled, tm.diskBytesSpilled)
    updateMetric(AggTaskMetrics.memoryBytesSpilled, tm.memoryBytesSpilled)

    // input / output
    updateMetric(AggTaskMetrics.inputBytesRead, tm.inputMetrics.bytesRead)
    updateMetric(AggTaskMetrics.inputRecordsRead, tm.inputMetrics.recordsRead)
    updateMetric(AggTaskMetrics.outputBytesWritten, tm.outputMetrics.bytesWritten)
    updateMetric(AggTaskMetrics.outputRecordsWritten, tm.outputMetrics.recordsWritten)

    // shuffle - read metrics
    updateMetric(AggTaskMetrics.shuffleReadFetchWaitTime, tm.shuffleReadMetrics.fetchWaitTime)
    updateMetric(AggTaskMetrics.shuffleReadBytesRead, tm.shuffleReadMetrics.totalBytesRead)
    updateMetric(AggTaskMetrics.shuffleReadRecordsRead, tm.shuffleReadMetrics.recordsRead)
    updateMetric(AggTaskMetrics.shuffleReadLocalBlocks, tm.shuffleReadMetrics.localBlocksFetched)
    updateMetric(AggTaskMetrics.shuffleReadRemoteBlocks, tm.shuffleReadMetrics.remoteBlocksFetched)

    // shuffle - write metrics
    updateMetric(AggTaskMetrics.shuffleWriteTime, tm.shuffleWriteMetrics.writeTime)
    updateMetric(AggTaskMetrics.shuffleWriteBytesWritten, tm.shuffleWriteMetrics.bytesWritten)
    updateMetric(AggTaskMetrics.shuffleWriteRecordsWritten, tm.shuffleWriteMetrics.recordsWritten)

    // tm.peakExecutionMemory - Peak memory used by internal data structures created during shuffles,
    // aggregations and joins. The value of this accumulator should be approximately the sum of the peak
    // sizes across all such data structures created in this task. For SQL jobs, this only tracks all unsafe
    // operators and ExternalSort.
    updateMetric(AggTaskMetrics.peakExecutionMemory, tm.peakExecutionMemory)
    updateMetric(AggTaskMetrics.taskDuration, ti.duration)

    if (ti.failed) failedTasks += 1
    else if (ti.killed) killedTasks += 1
    else completedTasks += 1

    count += 1
  }

  private def mergeMetric(metric: AggTaskMetrics.Metric, aggNewValue: AggregateValue): Unit = {
    val aggregateValue = map.getOrElse(metric, new AggregateValue)
    if (count == 0) {
      map(metric) = aggregateValue
    }
    aggregateValue.value += aggNewValue.value
    aggregateValue.max = math.max(aggregateValue.max, aggNewValue.max)
    aggregateValue.min = math.min(aggregateValue.min, aggNewValue.min)
    aggregateValue.mean = (aggregateValue.mean * aggregateValue.samples + aggNewValue.mean * aggNewValue.samples) / (aggregateValue.samples + aggNewValue.samples)
  }

  def update(metrics: AggTaskMetrics): Unit = synchronized {

    mergeMetric(AggTaskMetrics.executorDeserializeCpuTime, metrics.map(AggTaskMetrics.executorDeserializeCpuTime))
    mergeMetric(AggTaskMetrics.executorDeserializeTime, metrics.map(AggTaskMetrics.executorDeserializeTime))
    mergeMetric(AggTaskMetrics.executorCpuTime, metrics.map(AggTaskMetrics.executorCpuTime))
    mergeMetric(AggTaskMetrics.executorRunTime, metrics.map(AggTaskMetrics.executorRunTime))

    // other metrics
    // tm.jvmGCTime - Amount of time the JVM spent in garbage collection while executing this task
    // tm.resultSize - The number of bytes this task transmitted back to the driver as the TaskResult
    // tm.resultSerializationTime - Amount of time spent serializing the task result
    mergeMetric(AggTaskMetrics.jvmGCTime, metrics.map(AggTaskMetrics.jvmGCTime))
    mergeMetric(AggTaskMetrics.resultSize, metrics.map(AggTaskMetrics.resultSize))
    mergeMetric(AggTaskMetrics.resultSerializationTime, metrics.map(AggTaskMetrics.resultSerializationTime))

    // spill metrics
    // tm.diskBytesSpilled - The number of on-disk bytes spilled by this task
    // tm.memoryBytesSpilled - The number of in-memory bytes spilled by this task
    mergeMetric(AggTaskMetrics.diskBytesSpilled, metrics.map(AggTaskMetrics.resultSerializationTime))
    mergeMetric(AggTaskMetrics.memoryBytesSpilled, metrics.map(AggTaskMetrics.resultSerializationTime))

    // input / output
    mergeMetric(AggTaskMetrics.inputBytesRead, metrics.map(AggTaskMetrics.inputBytesRead))
    mergeMetric(AggTaskMetrics.inputRecordsRead, metrics.map(AggTaskMetrics.inputRecordsRead))
    mergeMetric(AggTaskMetrics.outputBytesWritten, metrics.map(AggTaskMetrics.outputBytesWritten))
    mergeMetric(AggTaskMetrics.outputRecordsWritten, metrics.map(AggTaskMetrics.outputRecordsWritten))

    // shuffle - read metrics
    mergeMetric(AggTaskMetrics.shuffleReadFetchWaitTime, metrics.map(AggTaskMetrics.shuffleReadFetchWaitTime))
    mergeMetric(AggTaskMetrics.shuffleReadBytesRead, metrics.map(AggTaskMetrics.shuffleReadBytesRead))
    mergeMetric(AggTaskMetrics.shuffleReadRecordsRead, metrics.map(AggTaskMetrics.shuffleReadRecordsRead))
    mergeMetric(AggTaskMetrics.shuffleReadLocalBlocks, metrics.map(AggTaskMetrics.shuffleReadLocalBlocks))
    mergeMetric(AggTaskMetrics.shuffleReadRemoteBlocks, metrics.map(AggTaskMetrics.shuffleReadRemoteBlocks))

    // shuffle - write metrics
    mergeMetric(AggTaskMetrics.shuffleWriteTime, metrics.map(AggTaskMetrics.shuffleWriteTime))
    mergeMetric(AggTaskMetrics.shuffleWriteBytesWritten, metrics.map(AggTaskMetrics.shuffleWriteBytesWritten))
    mergeMetric(AggTaskMetrics.shuffleWriteRecordsWritten, metrics.map(AggTaskMetrics.shuffleWriteRecordsWritten))

    // tm.peakExecutionMemory - Peak memory used by internal data structures created during shuffles,
    // aggregations and joins. The value of this accumulator should be approximately the sum of the peak
    // sizes across all such data structures created in this task. For SQL jobs, this only tracks all unsafe
    // operators and ExternalSort.
    mergeMetric(AggTaskMetrics.peakExecutionMemory, metrics.map(AggTaskMetrics.peakExecutionMemory))
    mergeMetric(AggTaskMetrics.taskDuration, metrics.map(AggTaskMetrics.taskDuration))

    failedTasks += metrics.failedTasks
    killedTasks += metrics.killedTasks
    completedTasks += metrics.completedTasks
    count += metrics.count
  }

  def getMetricMin(metric: AggTaskMetrics.Metric): Long = map(metric).min

  def getMetricMax(metric: AggTaskMetrics.Metric): Long = Try(map(metric).max).getOrElse(0L)

  def getMetricMean(metric: AggTaskMetrics.Metric): Double = map(metric).mean

  def getMetricSum(metric: AggTaskMetrics.Metric): Long = Try(map(metric).value).getOrElse(0L)


  def getIOMetrics: List[List[String]] = map
    .toList
    .filter(m => List("inputBytesRead", "inputRecordsRead", "outputBytesWritten", "outputRecordsWritten")
      .exists(_.equalsIgnoreCase(m._1.toString))
    )
    .sortBy(_._1)
    .map(x => formatMetric(x._1)(x))

  def getExecutionMetrics: List[List[String]] = map
    .toList
    .filter(m =>
      List("executorDeserializeCpuTime", "executorDeserializeTime", "executorCpuTime", "executorRunTime",
        "jvmGCTime", "resultSize", "resultSerializationTime", "peakExecutionMemory", "taskDuration"
      ).exists(_.equalsIgnoreCase(m._1.toString))
    )
    .sortBy(_._1)
    .map(x => formatMetric(x._1)(x))

  def getSpillMetrics: List[List[String]] = map
    .toList
    .filter(m => List("memoryBytesSpilled", "diskBytesSpilled").exists(_.equalsIgnoreCase(m._1.toString)))
    .sortBy(_._1)
    .map(x => formatMetric(x._1)(x))

  def getShuffleReadMetrics: List[List[String]] = map
    .toList
    .filter(_._1.toString.contains("shuffleRead"))
    .sortBy(_._1)
    .map(x => formatMetric(x._1)(x))

  def getShuffleWriteMetrics: List[List[String]] = map
    .toList
    .filter(_._1.toString.contains("shuffleWrite"))
    .sortBy(_._1)
    .map(x => formatMetric(x._1)(x))

}

object AggTaskMetrics extends Enumeration {

  val metricName: Map[String, String] = Map(
    "inputBytesRead" -> "Input Bytes Read",
    "inputRecordsRead" -> "Input Records Read",
    "outputBytesWritten" -> "Output Bytes Written",
    "outputRecordsWritten" -> "Output Records Written",

    "shuffleWriteTime" -> "Write Time",
    "shuffleWriteBytesWritten" -> "Bytes Written",
    "shuffleWriteRecordsWritten" -> "Records Written",
    "shuffleReadFetchWaitTime" -> "Fetch Wait Time",
    "shuffleReadBytesRead" -> "Bytes Read",
    "shuffleReadRecordsRead" -> "Records Read",
    "shuffleReadLocalBlocks" -> "Local Blocks Read",
    "shuffleReadRemoteBlocks" -> "Remote Blocks Read",

    "memoryBytesSpilled" -> "Memory Bytes Spilled",
    "diskBytesSpilled" -> "Disk Bytes Spilled",

    "executorDeserializeCpuTime" -> "Executor Deserialize CpuTime",
    "executorDeserializeTime" -> "Executor Deserialize Time",
    "executorCpuTime" -> "Executor CpuTime",
    "executorRunTime" -> "Executor Runtime",

    "jvmGCTime" -> "JVM GC Time",
    "resultSize" -> "Result Size",
    "resultSerializationTime" -> "Result Serialization Time",
    "peakExecutionMemory" -> "Peak Execution Memory",
    "taskDuration" -> "Task Duration"
  )

  type Metric = Value
  val
  executorDeserializeCpuTime,
  executorDeserializeTime,
  executorCpuTime,
  executorRunTime,

  jvmGCTime,
  resultSize,
  resultSerializationTime,
  peakExecutionMemory,
  taskDuration,

  diskBytesSpilled,
  memoryBytesSpilled,

  inputBytesRead,
  inputRecordsRead,
  outputBytesWritten,
  outputRecordsWritten,

  shuffleReadFetchWaitTime,
  shuffleReadBytesRead,
  shuffleReadRecordsRead,
  shuffleReadLocalBlocks,
  shuffleReadRemoteBlocks,

  shuffleWriteTime,
  shuffleWriteBytesWritten,
  shuffleWriteRecordsWritten
  = Value


}
