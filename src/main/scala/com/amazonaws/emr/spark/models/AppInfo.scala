package com.amazonaws.emr.spark.models

import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.utils.Constants.{DefaultRegion, NotAvailable}
import com.amazonaws.emr.spark.models.runtime.{JobRun, NotDetectedRun}
import org.apache.spark.utils.SparkSubmitHelper.SparkSubmitCommand
import software.amazon.awssdk.regions.Region

/**
 * AppInfo stores high-level metadata and insights about a Spark application.
 *
 * It captures basic identifying information (e.g., app ID, name, Spark version),
 * runtime window (start and end times), and key diagnostics like streaming flags,
 * runtime classification, and failure insights.
 *
 * This object is designed to be lightweight and serves as the primary summary
 * for use in reports and dashboards.
 *
 */
class AppInfo {

  /** Store the latest EMR available in the region */
  lazy val latestEmrRelease: Region => String = (region: Region) => {
    AwsEmr.latestRelease(region)
  }

  /** The environment and deployment context of the application (e.g., EMR on EC2, Serverless). */
  var runtime: JobRun = NotDetectedRun()

  /** The Spark application ID (e.g., application_123456789). */
  var applicationID: String = NotAvailable

  /** The name of the Spark application, as set via `spark.app.name`. */
  var sparkAppName: String = NotAvailable

  /** The version of Spark used to launch this application. */
  var sparkVersion: String = NotAvailable

  /** Start time in epoch milliseconds. */
  var startTime: Long = 0L

  /** End time in epoch milliseconds. */
  var endTime: Long = 0L

  /** Indicates whether the app is running Spark Streaming or Structured Streaming. */
  var isSparkStreaming: Boolean = false

  // ------------------------------------------------------------------------
  // Insights and diagnostics (rendered in report)
  // ------------------------------------------------------------------------

  /** Informational messages about performance or configuration choices. */
  val insightsInfo = new scala.collection.mutable.HashMap[String, String]

  /** Warning messages related to risk or performance bottlenecks. */
  val insightsWarn = new scala.collection.mutable.HashMap[String, String]

  /** Issues or failures detected during analysis (e.g., task failures). */
  val insightsIssue = new scala.collection.mutable.HashMap[String, String]

  /** Details of failed tasks, indexed by task attempt ID. */
  val insightsTaskFailures = new scala.collection.mutable.HashMap[Long, String]

  /** Parsed `spark-submit` command (if available from system configs). */
  var sparkCmd: Option[SparkSubmitCommand] = None

  /**
   * Computes the total runtime duration of the application.
   *
   * @return Duration in milliseconds (endTime - startTime)
   */
  def duration: Long = endTime - startTime

}