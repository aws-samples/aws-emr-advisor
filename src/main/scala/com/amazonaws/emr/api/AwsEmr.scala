package com.amazonaws.emr.api

import com.amazonaws.emr.Config
import com.amazonaws.emr.utils.Constants.NotAvailable
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.{DescribeSubnetsRequest, Subnet}
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model._
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * AwsEmr provides helper methods to interact with AWS EMR, EMR Serverless, and EC2 services.
 *
 * This utility is used to:
 *   - List available EMR release versions by region
 *   - Resolve EMR release labels by Spark version
 *   - Fetch Spark version for a given EMR release
 *   - Retrieve EC2 cluster metadata (e.g., instance lists, subnet details)
 *   - Locate EMR Serverless application context based on job run metadata
 *
 * It uses cached metadata to reduce AWS API calls where appropriate.
 */
object AwsEmr extends Logging {

  /** Cache of EMR releases by region (fetched once per region). */
  private val cachedReleases = new ConcurrentHashMap[Region, List[String]]()

  /** Hardcoded map of Spark versions to EMR releases based on known documentation. */
  private val cachedSparkRelease = Map(
    "3.5.5" -> List("emr-7.9.0"),
    "3.5.4" -> List("emr-7.8.0"),
    "3.5.3" -> List("emr-7.6.0", "emr-7.7.0"),
    "3.5.2" -> List("emr-7.5.0", "emr-7.4.0"),
    "3.5.1" -> List("emr-7.3.0", "emr-7.2.0"),
    "3.5.0" -> List("emr-7.1.0", "emr-7.0.0"),
    "3.4.1" -> List("emr-6.15.0", "emr-6.14.0", "emr-6.13.0"),
    "3.4.0" -> List("emr-6.12.0"),
    "3.3.2" -> List("emr-6.11.0"),
    "3.3.1" -> List("emr-6.10.0"),
    "3.3.0" -> List("emr-6.9.0", "emr-6.8.0"),
    "3.2.1" -> List("emr-6.7.0"),
    "3.2.0" -> List("emr-6.6.0"),
    "3.1.2" -> List("emr-6.5.0", "emr-6.4.0"),
    "3.1.1" -> List("emr-6.3.0"),
    "3.0.1" -> List("emr-6.2.0"),
    "3.0.0" -> List("emr-6.1.0")
  )

  /**
   * Lists available EMR release labels in a given region.
   *
   * @param region AWS region (default: us-east-1)
   * @return List of EMR release strings (e.g., "emr-6.10.0")
   */
  def releases(region: Region = Region.US_EAST_1): List[String] = {
    cachedReleases.computeIfAbsent(region, fetchReleases)
  }

  /** Helper to call `ListReleaseLabels` and cache the result. */
  private def fetchReleases(region: Region): List[String] = synchronized {
    val client = EmrClient.builder().region(region).build()
    val request = ListReleaseLabelsRequest.builder().build()
    val response = client.listReleaseLabels(request)
    response.releaseLabels().asScala.toList
  }

  /**
   * Returns the latest EMR release in the region, or "n/a" if none found.
   *
   * @param region AWS region (default: us-east-1)
   */
  def latestRelease(region: Region = Region.US_EAST_1): String = {
    releases(region).headOption.getOrElse(NotAvailable)
  }

  /**
   * Finds one or more EMR release labels that support the specified Spark version.
   *
   * @param sparkVersion Spark version (e.g., "3.4.1", "3.4.1-amzn-1")
   * @param region       AWS region
   * @return Matching EMR releases (e.g., "emr-6.14.0")
   */
  def findReleaseBySparkVersion(sparkVersion: String, region: Region = Region.US_EAST_1): List[String] = {
    logger.debug(s"Searching EMR releases for Spark $sparkVersion ($region)")
    val sparkBase = sparkVersion.split("-amzn")(0)
    cachedSparkRelease.getOrElse(
      sparkBase,
      emrReleaseSparkVersion(region = region)
        .collect { case (label, version) if sparkVersion.contains(version) => label }
        .toList
    )
  }

  /**
   * Discovers Spark versions available in recent EMR release labels.
   *
   * Filters by major versions and only includes base `.0` releases.
   *
   * @param emrMajorVersion List of EMR major versions to consider
   * @param region          AWS region
   * @return Map of EMR release label -> Spark version
   */
  private def emrReleaseSparkVersion(
    emrMajorVersion: List[String] = Config.EmrReleaseFilter,
    region: Region = Region.US_EAST_1
  ): Map[String, String] = {
    val client = EmrClient.builder().region(region).build()
    val cached = cachedSparkRelease.values.flatten.toSet
    val filtered = releases(region)
      .filter(label => !cached.contains(label))
      .filter(label => label.endsWith(".0") && emrMajorVersion.exists(label.contains))

    try {
      filtered.flatMap { label =>
        val req = DescribeReleaseLabelRequest.builder().releaseLabel(label).build()
        val resp = client.describeReleaseLabel(req)
        resp.applications().asScala.find(_.name() == "Spark").map(app => label -> app.version())
      }.toMap
    } finally {
      client.close()
    }
  }

  /**
   * Returns the Spark version used by a given EMR release.
   *
   * @param release EMR release label (e.g., "emr-6.14.0")
   * @param region  AWS region
   */
  def getSparkVersion(release: String, region: Region = Region.US_EAST_1): String = {
    val client = EmrClient.builder.region(region).build
    val request = DescribeReleaseLabelRequest.builder.releaseLabel(release).build
    val resp = client.describeReleaseLabel(request)
    resp.applications().asScala.find(_.name() == "Spark").map(_.version()).getOrElse(NotAvailable)
  }

  /**
   * Finds the EMR Serverless application matching the provided job run ID.
   *
   * @param applicationName Application name
   * @param jobRunId        EMR Serverless job run ID
   * @param region          AWS region
   * @return Optionally returns the matching application summary
   */
  def findServerlessApplicationByJobRun(
    applicationName: String,
    jobRunId: String,
    region: Region = Region.US_EAST_1
  ): Option[ApplicationSummary] = {
    val client = EmrServerlessClient.builder.region(region).build
    val request = ListApplicationsRequest.builder.build

    try {
      client
        .listApplications(request)
        .applications().asScala
        .filter(_.name() == applicationName)
        .flatMap { app =>
          val jobReq = GetJobRunRequest.builder.applicationId(app.id).jobRunId(jobRunId).build
          val jobRun = client.getJobRun(jobReq).jobRun()
          if (jobRun.jobRunId() == jobRunId) Some(app) else None
        }
        .headOption
    } catch {
      case t: Throwable =>
        logger.warn(s"Cannot find any valid application for JobRunId: $jobRunId ($region): ${t.getMessage}")
        None
    }
  }

  /**
   * Lists all EC2 instances for an EMR cluster.
   *
   * @param clusterId Cluster identifier
   * @param region    AWS region
   */
  def listInstances(clusterId: String, region: Region = Region.US_EAST_1): List[Instance] = {
    val client = EmrClient.builder.region(region).build
    val request = ListInstancesRequest.builder.clusterId(clusterId).build()
    try {
      client.listInstances(request).instances().asScala.toList
    } catch {
      case _: Throwable =>
        logger.error(s"Cannot retrieve instances for cluster: $clusterId ($region)")
        Nil
    }
  }

  /**
   * Returns full cluster metadata for a given EMR EC2 cluster ID.
   *
   * @param clusterId EMR cluster ID
   * @param region    AWS region
   */
  def describeEc2Cluster(clusterId: String, region: Region = Region.US_EAST_1): Cluster = {
    val client = EmrClient.builder.region(region).build
    val request = DescribeClusterRequest.builder().clusterId(clusterId).build()
    client.describeCluster(request).cluster()
  }

  /**
   * Describes the subnet given a subnet ID.
   *
   * @param subnetId Subnet identifier
   * @param region   AWS region
   * @return List of matching subnet details
   */
  def describeSubnet(subnetId: String, region: Region = Region.US_EAST_1): List[Subnet] = {
    val client = Ec2Client.builder().region(region).build()
    val request = DescribeSubnetsRequest.builder().subnetIds(subnetId).build()
    client.describeSubnets(request).subnets().asScala.toList
  }

}
