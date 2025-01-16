package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes, printDurationStr}
import org.apache.logging.log4j.scala.Logging

class AppInsightsAnalyzer extends AppAnalyzer with HtmlBase with Logging {

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logger.info("Generate application insights...")

    val executors = appContext.appSparkExecutors
    val totalSpilledBytes = executors.getTotalDiskBytesSpilled
    val totalShuffleBytesWritten = executors.getTotalShuffleBytesWritten

    // Base insights
    appContext.appInfo.insightsInfo("base_io") = htmlTextInfo(
      s"Your application read <b>${humanReadableBytes(executors.getTotalInputBytesRead)}</b> as " +
        s"input, and wrote a total of <b>${humanReadableBytes(executors.getTotalOutputBytesWritten)}</b> as output"
    )

    if (appContext.appMetrics.getFailedTasks != 0)
      appContext.appInfo.insightsIssue("base_proc_fail") = htmlTextIssue(
        s"There were <b>${appContext.appMetrics.getFailedTasks}</b> " +
          s"failed tasks in your job"
      )

    if (totalSpilledBytes > 0L) {
      appContext.appInfo.insightsWarn("base_spilled_io") = htmlTextWarning(
        s"Your application spilled a total of <b>${humanReadableBytes(totalSpilledBytes)}</b> on local disks"
      )
    }

    if (totalShuffleBytesWritten > 0L) {
      appContext.appInfo.insightsInfo("base_shuffle_io") = htmlTextInfo(s"Your application wrote a total of <b>" +
        s"${humanReadableBytes(totalShuffleBytesWritten)}</b> shuffle data on local disks")
    }

    if (appContext.appSparkExecutors.isComputeIntensive)
      appContext.appInfo.insightsInfo("base_type") = htmlTextInfo(s"Your application is <b>Compute</b> intensive")

    if (appContext.appSparkExecutors.isMemoryIntensive)
      appContext.appInfo.insightsInfo("base_type") = htmlTextInfo(s"Your application is <b>Memory</b> intensive")

    // Spark Driver
    if (appContext.appConfigs.driverMemory >= byteStringAsBytes("10g"))
      appContext.appInfo.insightsWarn("base_driver") = htmlTextWarning(
        s"The Spark driver was launched with <b>${humanReadableBytes(appContext.appConfigs.driverMemory)}</b>. " +
          s"Unless strictly required, consider reducing this value"
      )

    // Spark Executors
    appContext.appInfo.insightsInfo("executor_info") = htmlTextInfo(executors.summary)
    appContext.appInfo.insightsInfo("executor_info2") = htmlTextInfo(executors.summaryResources)
    appContext.appInfo.insightsInfo("executor_launch") = htmlTextInfo(
      s"The maximum launch time for an executor was <b>${printDurationStr(executors.getMaxLaunchTime)}</b>"
    )

    if (!appContext.appSparkExecutors.isShuffleWriteUniform)
      appContext.appInfo.insightsWarn("shuffle_write") =
        htmlTextWarning("Shuffle writes are not uniform across executors")

    //=================================================================================================
    // EMR Insights
    //=================================================================================================
    val latestEmr = AwsEmr.latestRelease()
    val deployment = appContext.appInfo.runtime
    if (deployment.isRunningOnEmr && !deployment.release.contains(latestEmr)) {
      appContext.appInfo.insightsWarn("emr_version") = htmlTextWarning(
        s"The application might run faster using the latest EMR release available (${htmlBold(latestEmr)})"
      )
    }

  }

}
