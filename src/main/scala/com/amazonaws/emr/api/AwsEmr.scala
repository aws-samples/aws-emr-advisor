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

import scala.collection.JavaConverters.asScalaBufferConverter

object AwsEmr extends Logging {

  /**
   * Return a list of available EMR releases in the specified AWS region
   *
   * @param region AWS Region
   */
  def releases(region: Region = Region.US_EAST_1): List[String] = {
    val client = EmrClient.builder.region(region).build
    val request = ListReleaseLabelsRequest.builder.build
    val response = client.listReleaseLabels(request)
    response.releaseLabels().asScala.toList
  }

  /**
   * Retrieve the latest EMR release
   *
   * @param region AWS Region
   */
  def latestRelease(region: Region = Region.US_EAST_1): String = {
    releases(region).headOption.getOrElse(NotAvailable)
  }

  /**
   * Retrieve the emr release label using the Spark version
   *
   * @param sparkVersion e.g 	3.4.1-amzn-1, 3.4.1
   */
  def findReleaseBySparkVersion(sparkVersion: String): List[String] = {
    emrReleaseSparkVersion()
      .filter(v => sparkVersion.contains(v._2))
      .keys
      .toList
  }

  def getSparkVersion(release: String, region: Region = Region.US_EAST_1): String = {
    val client = EmrClient.builder.region(region).build
    val request = DescribeReleaseLabelRequest.builder.releaseLabel(release).build
    val resp = client.describeReleaseLabel(request)
    resp.applications().asScala.toList.filter(a => a.name() == "Spark").map(_.version()).head
  }

  private def emrReleaseSparkVersion(
    emrMajorVersion: String = Config.EmrReleaseFilter,
    region: Region = Region.US_EAST_1
  ): Map[String, String] = {
    val releaseList = releases(region).filter(_.contains(emrMajorVersion))
    val client = EmrClient.builder.region(region).build
    releaseList.map { label =>
      val request = DescribeReleaseLabelRequest.builder.releaseLabel(label).build
      val resp = client.describeReleaseLabel(request)
      val sparkVersion = resp.applications().asScala.toList.filter(a => a.name() == "Spark").map(_.version()).head
      label -> sparkVersion
    }.toMap
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
   *
   * @param clusterId
   * @param region
   * @return
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
   *
   * @param clusterId
   * @param region
   * @return
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
