package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.utils.Constants.{DefaultRegion, ParamRegion}
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes, printDurationStr}
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.regions.Region

/**
 * AppInsightsAnalyzer generates high-level diagnostic insights from a Spark application's execution context.
 *
 * It produces user-friendly summaries and warnings related to:
 *   - I/O characteristics (input/output/shuffle)
 *   - Executor resource patterns
 *   - Driver configuration anomalies
 *   - Task failures and skew detection
 *   - EMR version usage (relative to latest)
 *
 * These insights are designed for inclusion in diagnostic reports and UIs,
 * enabling users to quickly understand performance traits or configuration risks.
 *
 * Highlights are rendered using the `HtmlBase` formatting helpers (e.g., `htmlTextInfo`, `htmlTextWarning`),
 * which produce annotated blocks for display in HTML reports.
 *
 */
class AppInsightsAnalyzer extends AppAnalyzer with HtmlBase with Logging {

  /**
   * Analyze the application context to derive high-level insights.
   *
   * @param appContext Application metrics, configs, and execution context.
   * @param startTime  Application start time in epoch ms.
   * @param endTime    Application end time in epoch ms.
   * @param options    Optional flags (e.g., region override).
   */
  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logger.info("Generate application insights...")

    val awsRegion = options.getOrElse(ParamRegion.name, DefaultRegion)

    val executors = appContext.appSparkExecutors
    val totalSpilledBytes = executors.getTotalDiskBytesSpilled
    val totalShuffleBytesWritten = executors.getTotalShuffleBytesWritten

    // Base I/O insight
    appContext.appInfo.insightsInfo("base_io") = htmlTextInfo(
      s"Your application read <b>${humanReadableBytes(executors.getTotalInputBytesRead)}</b> as " +
        s"input, and wrote a total of <b>${humanReadableBytes(executors.getTotalOutputBytesWritten)}</b> as output"
    )

    // Task failure insight
    if (appContext.appMetrics.getFailedTasks != 0)
      appContext.appInfo.insightsIssue("base_proc_fail") = htmlTextIssue(
        s"There were <b>${appContext.appMetrics.getFailedTasks}</b> " +
          s"failed tasks in your job"
      )

    // Spill insight
    if (totalSpilledBytes > 0L) {
      appContext.appInfo.insightsWarn("base_spilled_io") = htmlTextWarning(
        s"Your application spilled a total of <b>${humanReadableBytes(totalSpilledBytes)}</b> on local disks"
      )
    }

    // Shuffle insight
    if (totalShuffleBytesWritten > 0L) {
      appContext.appInfo.insightsInfo("base_shuffle_io") = htmlTextInfo(s"Your application wrote a total of <b>" +
        s"${humanReadableBytes(totalShuffleBytesWritten)}</b> shuffle data on local disks")
    }

    // Application workload classification
    if (appContext.appSparkExecutors.isComputeIntensive)
      appContext.appInfo.insightsInfo("base_type") = htmlTextInfo(s"Your application is <b>Compute</b> intensive")

    if (appContext.appSparkExecutors.isMemoryIntensive)
      appContext.appInfo.insightsInfo("base_type") = htmlTextInfo(s"Your application is <b>Memory</b> intensive")

    // Driver memory warning
    if (appContext.appConfigs.driverMemory >= byteStringAsBytes("10g"))
      appContext.appInfo.insightsWarn("base_driver") = htmlTextWarning(
        s"The Spark driver was launched with <b>${humanReadableBytes(appContext.appConfigs.driverMemory)}</b>. " +
          s"Unless strictly required, consider reducing this value"
      )

    // Executor resource summaries
    appContext.appInfo.insightsInfo("executor_info") = htmlTextInfo(executors.summary)
    appContext.appInfo.insightsInfo("executor_info2") = htmlTextInfo(executors.summaryResources)
    appContext.appInfo.insightsInfo("executor_launch") = htmlTextInfo(
      s"The maximum launch time for an executor was <b>${printDurationStr(executors.getMaxLaunchTime)}</b>"
    )

    // Shuffle skew warning
    if (!appContext.appSparkExecutors.isShuffleWriteUniform)
      appContext.appInfo.insightsWarn("shuffle_write") =
        htmlTextWarning("Shuffle writes are not uniform across executors")

    // EMR version suggestion
    val latestEmr = appContext.appInfo.latestEmrRelease(Region.of(awsRegion))
    val deployment = appContext.appInfo.runtime
    if (deployment.isRunningOnEmr && !deployment.release.contains(latestEmr)) {
      appContext.appInfo.insightsWarn("emr_version") = htmlTextWarning(
        s"The application might run faster using the latest EMR release available (${htmlBold(latestEmr)})"
      )
    }

  }

}
