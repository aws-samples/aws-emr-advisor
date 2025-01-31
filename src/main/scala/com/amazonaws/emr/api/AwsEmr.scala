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

object AwsEmr extends Logging {

  private val cachedReleases = new ConcurrentHashMap[Region, List[String]]()
  private val cachedSparkRelease = Map(
    "3.5.3" -> List("emr-7.6.0"),
    "3.5.2" -> List("emr-7.5.0","emr-7.4.0"),
    "3.5.1" -> List("emr-7.3.0","emr-7.2.0"),
    "3.5.0" -> List("emr-7.1.0","emr-7.0.0"),
    "3.4.1" -> List("emr-6.15.0","emr-6.14.0","emr-6.13.0"),
    "3.4.0" -> List("emr-6.12.0"),
    "3.3.2" -> List("emr-6.11.0"),
    "3.3.1" -> List("emr-6.10.0"),
    "3.3.0" -> List("emr-6.9.0","emr-6.8.0"),
    "3.2.1" -> List("emr-6.7.0"),
    "3.2.0" -> List("emr-6.6.0"),
    "3.1.2" -> List("emr-6.5.0","emr-6.4.0"),
    "3.1.1" -> List("emr-6.3.0"),
    "3.0.1" -> List("emr-6.2.0"),
    "3.0.0" -> List("emr-6.1.0"),
  )

  /**
   * Return a list of available EMR releases in the specified AWS region
   *
   * @param region AWS Region
   */
  def releases(region: Region = Region.US_EAST_1): List[String] = {
    cachedReleases.computeIfAbsent(region, fetchReleases)
  }

  private def fetchReleases(region: Region): List[String] = synchronized {
    val client = EmrClient.builder().region(region).build()
    val request = ListReleaseLabelsRequest.builder().build()
    val response = client.listReleaseLabels(request)
    response.releaseLabels().asScala.toList
  }

  /**
   * Retrieve the latest EMR release
   */
  def latestRelease(region: Region = Region.US_EAST_1): String = {
    releases(region).headOption.getOrElse(NotAvailable)
  }

  /**
   * Retrieve the emr release label using the Spark version.
   *
   * Note: Emr serverless only publish first version of a release in the doc (e.g 6.8.0, 6.9.0)
   *       minor versions are not published so we skip them.
   *
   * @param sparkVersion e.g 	3.4.1-amzn-1, 3.4.1
   */
  def findReleaseBySparkVersion(sparkVersion: String, region: Region = Region.US_EAST_1): List[String] = {

    val sparkBase = sparkVersion.split("-amzn")(0)

    if(cachedSparkRelease.contains(sparkBase)) cachedSparkRelease(sparkBase)
    else emrReleaseSparkVersion(region = region)
      .collect { case (label, version) if sparkVersion.contains(version) => label }
      .toList

  }

  private def emrReleaseSparkVersion(
    emrMajorVersion: String = Config.EmrReleaseFilter,
    region: Region = Region.US_EAST_1): Map[String, String] = {

    val releaseList = releases(region)
      .filter(label => label.contains(emrMajorVersion) && label.endsWith(".0"))

    val client = EmrClient.builder().region(region).build()

    try {
      releaseList.flatMap { label =>
        val request = DescribeReleaseLabelRequest.builder().releaseLabel(label).build()
        val response = client.describeReleaseLabel(request)

        response.applications().asScala
          .find(_.name() == "Spark")
          .map(app => label -> app.version())
      }.toMap
    } finally {
      client.close()
    }
  }

  def getSparkVersion(release: String, region: Region = Region.US_EAST_1): String = {
    val client = EmrClient.builder.region(region).build
    val request = DescribeReleaseLabelRequest.builder.releaseLabel(release).build
    val resp = client.describeReleaseLabel(request)
    resp.applications().asScala.toList.filter(a => a.name() == "Spark").map(_.version()).head
  }

  /**
   * Retrieve Emr Serverless application
   *
   * @param applicationName application Id where the job was submitted
   * @param jobRunId        job run Id
   * @param region          AWS Region
   */
  def findServerlessApplicationByJobRun(
    applicationName: String,
    jobRunId: String,
    region: Region = Region.US_EAST_1): Option[ApplicationSummary] = {

    val client = EmrServerlessClient.builder.region(region).build
    val request = ListApplicationsRequest.builder.build
    try {
      client
        .listApplications(request)
        .applications().asScala
        .filter(_.name().equals(applicationName))
        .map { app =>
          val jobRunsReq = GetJobRunRequest.builder.applicationId(app.id).jobRunId(jobRunId).build
          val jobRunsResp = client.getJobRun(jobRunsReq).jobRun()
          if (jobRunsResp.jobRunId().equals(jobRunId)) Some(app)
          else None
        }
        .find(_.nonEmpty)
        .head
    } catch {
      case _: Throwable =>
        logger.warn(s"Cannot find any valid application for JobRunId: $jobRunId ($region)")
        None
    }

  }

  /**
   * Return the list of instances in the cluster
   */
  def listInstances(clusterId: String, region: Region = Region.US_EAST_1): List[Instance] = {
    val client = EmrClient.builder.region(region).build
    val request = ListInstancesRequest.builder.clusterId(clusterId).build()
    try {
      client.listInstances(request)
        .instances()
        .asScala
        .toList
    } catch {
      case _: Throwable =>
        logger.error(s"Cannot retrieve instances for cluster: $clusterId ($region)")
        Nil
    }
  }

  /**
   * Describe an EMR EC2 cluster
   */
  def describeEc2Cluster(clusterId: String, region: Region = Region.US_EAST_1): Cluster = {
    val client = EmrClient.builder.region(region).build
    val request = DescribeClusterRequest.builder().clusterId(clusterId).build()
    client.describeCluster(request).cluster()
  }

  def describeSubnet(subnetId: String, region: Region = Region.US_EAST_1): List[Subnet] = {
    val client = Ec2Client.builder().region(region).build()
    val request = DescribeSubnetsRequest.builder().subnetIds(subnetId).build()
    client.describeSubnets(request).subnets().asScala.toList
  }

}
