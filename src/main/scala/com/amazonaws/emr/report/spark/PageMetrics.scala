package com.amazonaws.emr.report.spark

import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.report.HtmlReport.{htmlBoxInfo, htmlNavTabs, htmlTable, htmlTablePaginated}
import com.amazonaws.emr.spark.models.{AppEfficiency, AppMetrics, AppSparkExecutors}
import com.amazonaws.emr.spark.models.metrics.{AggExecutorMetrics, AggTaskMetrics}
import com.amazonaws.emr.spark.models.metrics.AggTaskMetrics._
import com.amazonaws.emr.spark.models.timespan.JobTimeSpan
import com.amazonaws.emr.utils.Formatter.{humanReadableBytes, pcm, printDate, printDuration, printTime}

import scala.util.Try

class PageMetrics(
  appMetrics: AppMetrics,
  jobMap: Map[Long, JobTimeSpan],
  appSparkExecutors: AppSparkExecutors,
  appEfficiency: AppEfficiency) extends HtmlPage {

  def taskTab: String = {
    val ioTable = htmlTable(
      List("Name", "Sum", "Min", "Mean", "Max"),
      appMetrics.appAggMetrics.getIOMetrics, CssTableStyle)

    val spilledTable = htmlTable(
      List("Name", "Sum", "Min", "Mean", "Max"),
      appMetrics.appAggMetrics.getSpillMetrics, CssTableStyle)

    val executionTable = htmlTable(
      List("Name", "Sum", "Min", "Mean", "Max"),
      appMetrics.appAggMetrics.getExecutionMetrics, CssTableStyle)

    val shuffleReadTable = htmlTable(
      List("Name", "Sum", "Min", "Mean", "Max"),
      appMetrics.appAggMetrics.getShuffleReadMetrics, CssTableStyle)

    val shuffleWriteTable = htmlTable(
      List("Name", "Sum", "Min", "Mean", "Max"),
      appMetrics.appAggMetrics.getShuffleWriteMetrics, CssTableStyle)

    s"""<h5 class="mt-3">Input/Output</h5>
       |$ioTable
       |
       |<h5 class="mt-3">Data Spill</h5>
       |$spilledTable
       |
       |<h5 class="mt-3">Execution</h5>
       |$executionTable
       |
       |<h5 class="mt-3">Shuffle Read</h5>
       |$shuffleReadTable
       |
       |<h5 class="mt-3">Shuffle Write</h5>
       |$shuffleWriteTable
       |
       |<p class="mt-3">${htmlBoxInfo("Aggregated statistics across all Spark Jobs and Stages")}</p>
       |""".stripMargin
  }

  def execTab: String = {

    val runtimeTable = htmlTable(
      List("Name", "Runtime", "Percentage"),
      List(
        List("Driver", printDuration(appEfficiency.driverTime), f"""${appEfficiency.driverTimePercentage}%3.2f%%"""),
        List("Executor", printDuration(appEfficiency.executorsTime), f"""${appEfficiency.executorsTimePercentage}%3.2f%%"""),
        List("Total", printDuration(appEfficiency.appTotalTime), "")
      ), CssTableStyle)

    val estimatedTable = htmlTable(
      List("Name", "Runtime"),
      List(
        List("Execution time with infinite resources", printDuration(appEfficiency.executionTimeInfiniteResources)),
        List("Execution time with perfect parallelism no skew", printDuration(appEfficiency.executionTimePerfectParallelism)),
        List("Execution time with single executor and single core", pcm(appEfficiency.executionTimeSingleExecutorOneCore))
      ), CssTableStyle)

    // Spark Executors Memory
    val executorMemoryTable = htmlTable(
      List("Name", "Single Executor (Max)"),
      List(
        List("Memory Peak", s"${humanReadableBytes(appEfficiency.executorPeakMemoryBytes)}"),
        List("Memory Wasted", s"${humanReadableBytes(appEfficiency.executorWastedMemoryBytes)}"),
        List("Memory Spilled to Disk", s"${humanReadableBytes(appEfficiency.executorSpilledMemoryBytes)}"),
      ), CssTableStyle)

    s"""<div class="metrics mt-3">
       |  <div class="row">
       |    <div class="col-3">
       |      $runtimeTable
       |    </div>
       |    <div class="col-3">
       |      $executorMemoryTable
       |    </div>
       |    <div class="col-6">
       |      $estimatedTable
       |    </div>
       |  </div>
       |</div>
       |
       |$executorsTable
       |""".stripMargin
  }

  def jobsTab: String = {
    val jobs = jobMap.toList.sortBy(_._2.duration.getOrElse(0L)).reverse
    val jobsData = jobs.map { j =>
      List(
        j._1.toString,
        j._2.duration.getOrElse(0L),
        j._2.stageMap.size,
        j._2.jobMetrics.count,
        j._2.jobMetrics.getMetricMean(AggTaskMetrics.taskDuration).toLong,
        j._2.jobMetrics.getMetricMax(AggTaskMetrics.taskDuration)
      ).map(_.toString)
    }
    htmlTablePaginated("jobsMetrics",
      List("Job Id", "Duration", "Stages", "Tasks", "Avg Task Duration", "Max Task Duration"),
      jobsData, "table-bordered table-striped table-sm jobs mt-3", List("Duration", "Avg Task Duration", "Max Task Duration"))
  }

  override def render: String = {
    val htmlTabs = Seq(
      ("execMetrics", "Executors", execTab),
      ("jobsMetrics", "Jobs", jobsTab),
      ("taskMetrics", "Tasks", taskTab)
    )
    htmlNavTabs("metricsTabs", htmlTabs, "execMetrics", "nav-pills border navbar-light bg-light", "mt-4 text-break")
  }

  private def executorsTable: String = {

    val data = appSparkExecutors.executors.toSeq.sortBy(x => Try(x._2.executorID.toInt).getOrElse(0)).map { x =>

      // time
      val activeDuration = x._2.endTime - x._2.startTime
      val launchDuration = x._2.startTime - x._2.executorInfo.requestTime.getOrElse(x._2.startTime)
      // memory
      val peakJvmMemoryOnHeap = x._2.executorMetrics.getMetricMax(AggExecutorMetrics.JVMHeapMemory)
      val peakJvmMemoryOffHeap = x._2.executorMetrics.getMetricMax(AggExecutorMetrics.JVMOffHeapMemory)

      List(
        x._2.executorID,
        x._2.hostID,
        s"""<div class="">${printTime(x._2.startTime)}</div><div class="fw-light">${printDate(x._2.startTime)}</div>""",
        s"""<div class="">${printTime(x._2.endTime)}</div><div class="fw-light">${printDate(x._2.endTime)}</div>""",
        printDuration(launchDuration),
        printDuration(activeDuration),
        x._2.executorInfo.totalCores,
        s"${humanReadableBytes(peakJvmMemoryOnHeap)} / ${humanReadableBytes(peakJvmMemoryOffHeap)}",
        x._2.executorTaskMetrics.failedTasks,
        x._2.executorTaskMetrics.completedTasks,
        humanReadableBytes(x._2.executorTaskMetrics.getMetricSum(inputBytesRead)),
        humanReadableBytes(x._2.executorTaskMetrics.getMetricSum(outputBytesWritten)),
        humanReadableBytes(x._2.executorTaskMetrics.getMetricSum(shuffleReadBytesRead)),
        humanReadableBytes(x._2.executorTaskMetrics.getMetricSum(shuffleWriteBytesWritten))
      ).map(_.toString)
    }.toList

    val header = List(
      "Executor ID", "Address", "Started", "Terminated", "Launch Time", "Active Time", "Cores",
      "Peak JVM Memory OnHeap / OffHeap", "Failed Tasks", "Completed Tasks", "Input", "Output",
      "Shuffle Read", "Shuffle Write"
    )

    htmlTablePaginated("executorTable", header, data, "table-bordered table-striped text-center mt-3")

  }

}
