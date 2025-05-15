package com.amazonaws.emr.spark.analyzer

import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.runtime._
import com.amazonaws.emr.utils.Constants.{DefaultRegion, NotAvailable, ParamRegion}
import com.amazonaws.services.costandusagereport.model.AWSRegion
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.utils.SparkHelper.parseSparkCmd
import software.amazon.awssdk.regions.Region

class AppRuntimeAnalyzer extends AppAnalyzer with Logging {

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long, options: Map[String, String]): Unit = {

    logger.info("Analyze Deployment Runtime...")

    val sparkVersion = appContext.appConfigs.sparkVersion

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

    def isEmrOnEks: Boolean = appContext.appConfigs.sparkConfigs.exists(_._1.contains("emr-containers"))

    def isEmrServerless: Boolean = appContext.appConfigs.sparkConfigs.exists(_._1.contains("emr-serverless"))

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

    // Parse Submitted application
    val cmd = appContext.appConfigs.systemConfigs.get("sun.java.command")
    if (cmd.nonEmpty) {

      // quick fix for when lakeformation.enabled parameter is not correct in Java command
      val cmdStr = cmd.get.replace("spark.emr-serverless.lakeformation.enabled=",
        "spark.emr-serverless.lakeformation.enabled=false")

      appContext.appInfo.sparkCmd = Some(parseSparkCmd(cmdStr))

    }
  }

}
