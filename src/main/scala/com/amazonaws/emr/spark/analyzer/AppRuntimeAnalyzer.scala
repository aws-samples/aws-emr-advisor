package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.runtime._
import com.amazonaws.emr.utils.Constants.{DefaultRegion, NotAvailable, ParamRegion}
import com.amazonaws.services.costandusagereport.model.AWSRegion
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.utils.SparkSubmitHelper
import software.amazon.awssdk.regions.Region

/**
 * AppRuntimeAnalyzer identifies the runtime deployment context of a Spark application.
 *
 * It attempts to classify the environment in which the application was executed as:
 *   - EMR on EC2
 *   - EMR on EKS
 *   - EMR Serverless
 *   - Unknown (NotDetectedRun)
 *
 * The analyzer extracts deployment metadata such as:
 *   - Cluster ID (for EC2)
 *   - Application ID (for Serverless)
 *   - Spark version
 *   - EMR release label
 *   - AWS region
 *
 * It also parses the original `spark-submit` command (if available) from
 * the system configs and attaches a parsed representation (`SparkSubmitCommand`)
 * to the application context.
 *
 */
class AppRuntimeAnalyzer extends AppAnalyzer with Logging {

  /**
   * Detects the Spark deployment type and extracts runtime metadata.
   *
   * @param appContext The full application context containing system and Spark configs.
   * @param startTime  The application start time (unused in this analyzer).
   * @param endTime    The application end time (unused in this analyzer).
   * @param options    Optional settings, including region override.
   */
  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logger.info("Analyze Deployment Runtime...")

    val sparkVersion = appContext.appConfigs.sparkVersion

    /** Detects if the app was run on EMR on EC2 (via known system/env variables or committers). */
    def isEmrOnEc2: Boolean = {
      // In client mode we can use ENV variables available on the master to detect
      // both release and cluster id
      val clientMode: Boolean = appContext.appConfigs.systemConfigs.exists(k =>
        k._1.equalsIgnoreCase("EMR_RELEASE_LABEL") ||
          k._1.equalsIgnoreCase("EMR_CLUSTER_ID"))
      // In cluster mode can't really use much. We'll just detect if the cluster is running on YARN
      // and validate that is EMR by checking one of the private extensions
      val clusterMode: Boolean = appContext.appConfigs.sparkConfigs.exists(k =>
        k._2.equalsIgnoreCase("yarn")) && appContext.appConfigs.sparkConfigs.exists(k =>
        k._2.equalsIgnoreCase("com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter"))

      clientMode || clusterMode
    }

    /** Detects if the app was run on EMR on EKS by checking config keys. */
    def isEmrOnEks: Boolean = appContext.appConfigs.sparkConfigs.exists(_._1.contains("emr-containers"))

    /** Detects if the app was run on EMR Serverless by checking for relevant Spark properties. */
    def isEmrServerless: Boolean = appContext.appConfigs.sparkConfigs.exists(_._1.contains("emr-serverless"))

    // Detect and construct appropriate JobRun metadata
    val jobRun: JobRun = if (isEmrOnEc2) {
      val clusterId = appContext.appConfigs.systemConfigs.getOrElse("EMR_CLUSTER_ID", NotAvailable)
      val releaseLabel = appContext.appConfigs.systemConfigs.get("EMR_RELEASE_LABEL")
      val region = AWSRegion.values()
        .map(r => r.toString).toList
        .find(r => appContext.appConfigs.sparkConfigs.values.toList.exists(p => p matches s".*$r.*"))
        .getOrElse(DefaultRegion)

      EmrOnEc2Run(clusterId, sparkVersion, releaseLabel, region)

    } else if (isEmrOnEks) {
      val region = AWSRegion.values()
        .map(r => r.toString).toList
        .find(r => appContext.appConfigs.sparkConfigs.values.toList.exists(p => p matches s".*$r.*"))
        .getOrElse(DefaultRegion)

      EmrOnEksRun(sparkVersion, None, region)

    } else if (isEmrServerless) {
      val appName = appContext.appConfigs.sparkConfigs.getOrElse("spark.app.name", NotAvailable)
      val jobRunId = appContext.appConfigs.sparkConfigs.getOrElse("spark.app.id", NotAvailable)
      val region = appContext.appConfigs.sparkConfigs.getOrElse(
        "spark.hadoop.aws.region", options.getOrElse(ParamRegion.name, DefaultRegion)
      )
      val application = AwsEmr.findServerlessApplicationByJobRun(appName, jobRunId, Region.of(region))

      EmrServerlessRun(jobRunId, sparkVersion, application, region)

    } else NotDetectedRun()

    appContext.appInfo.runtime = jobRun

    // Parse spark-submit command (if available)
    val cmdOpt = appContext.appConfigs.systemConfigs.get("sun.java.command")
    if (cmdOpt.nonEmpty) {
      val sanitizedCmd = cmdOpt.get.replace(
        "spark.emr-serverless.lakeformation.enabled=",
        "spark.emr-serverless.lakeformation.enabled=false"
      )
      appContext.appInfo.sparkCmd = Some(SparkSubmitHelper.parse(sanitizedCmd))
    }
  }
}
