package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.utils.Constants._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emrserverless.model._

/**
 * Trait representing the runtime deployment context of a Spark application.
 *
 * A `JobRun` encapsulates metadata about where and how the Spark job was executed,
 * including environment-specific properties such as cluster IDs, EMR release versions,
 * AWS regions, and links to documentation or consoles.
 *
 * Implementing types include:
 *   - [[EmrOnEc2Run]]       → EMR on EC2
 *   - [[EmrOnEksRun]]       → EMR on EKS
 *   - [[EmrServerlessRun]]  → EMR Serverless
 *   - [[NotDetectedRun]]    → Unknown/unsupported environments
 */
trait JobRun {

  /** Human-readable name of the deployment environment. */
  val name: String

  /** Link to relevant EMR documentation for this deployment type. */
  val documentationUrl: String

  /** Returns the base URL to the EMR release notes for a given version. */
  def baseReleaseUrl(version: String): String

  /** Returns a formatted HTML representation of the deployment name with a doc link. */
  def deployment: String = {
    if (documentationUrl.nonEmpty) s"""<a target="_blank" href="$documentationUrl">$name</a>"""
    else name
  }

  /** A concise string describing the deployment type. */
  def deploymentInfo: String

  /** Indicates whether the job was detected to run on an EMR-managed environment. */
  def isRunningOnEmr: Boolean

  /** EMR release labels associated with this run (possibly inferred from Spark version). */
  def release: List[String]

  /** Returns an HTML-formatted string with links to relevant EMR release docs. */
  def releaseInfo(): String = {
    if (release.nonEmpty) {
      release
        .sorted
        .map(r => s"""<a target="_blank" href="${baseReleaseUrl(r)}">$r</a>""")
        .mkString(" / ")
    } else {
      NotAvailable
    }
  }
}

/**
 * Represents a Spark job that was run on Amazon EMR (EC2-based deployment).
 *
 * @param clusterId     The EMR cluster ID (e.g., j-xxxxxx).
 * @param sparkVersion  The version of Spark used.
 * @param releaseLabel  The EMR release label (e.g., emr-6.10.0), if available.
 * @param region        AWS region where the job was executed.
 */
case class EmrOnEc2Run(
  clusterId: String,
  sparkVersion: String,
  releaseLabel: Option[String],
  region: String = DefaultRegion
) extends JobRun {

  private val clusterUrl = s"https://$region.console.aws.amazon.com/emr/home?region=$region#/clusterDetails/$clusterId"

  override val documentationUrl: String = LinkEmrOnEc2Documentation
  override val name: String = "Emr On Ec2"

  override def baseReleaseUrl(version: String): String = {
    val cleanedRelease = version.replaceAll("\\.", "")
    s"""https://docs.aws.amazon.com/emr/latest/ReleaseGuide/$cleanedRelease-release.html"""
  }

  override def deploymentInfo: String = {
    if (region.nonEmpty && !clusterId.equalsIgnoreCase(NotAvailable)) {
      s"""$deployment / <a target="_blank" href="$clusterUrl">$clusterId</a>"""
    } else {
      s"$deployment"
    }
  }

  override def isRunningOnEmr: Boolean = true

  override def release: List[String] = {
    if (releaseLabel.nonEmpty) List(releaseLabel.get)
    else AwsEmr.findReleaseBySparkVersion(sparkVersion, Region.of(region))
  }
}

/**
 * Represents a Spark job run on Amazon EMR on EKS (Kubernetes-based).
 *
 * @param sparkVersion  The version of Spark used.
 * @param releaseLabel  The EMR release label (optional).
 * @param region        AWS region where the EKS cluster is located.
 */
case class EmrOnEksRun(
  sparkVersion: String,
  releaseLabel: Option[String],
  region: String = DefaultRegion
) extends JobRun {

  override val name: String = "Emr On Eks"

  override val documentationUrl: String = LinkEmrOnEksDocumentation

  override def baseReleaseUrl(version: String): String = {
    val cleanedRelease = version.replaceAll("emr-", "emr-eks-")
    s"""https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/$cleanedRelease.html"""
  }

  override def deploymentInfo: String = s"$deployment"

  override def isRunningOnEmr: Boolean = true

  override def release: List[String] = {
    if (releaseLabel.nonEmpty) List(releaseLabel.get)
    else AwsEmr.findReleaseBySparkVersion(sparkVersion, Region.of(region))
  }
}

/**
 * Represents a Spark job run on Amazon EMR Serverless.
 *
 * @param jobRunId    The unique EMR Serverless job run ID.
 * @param sparkVersion The Spark version used in the job.
 * @param appSummary  Optional metadata from `ListApplications`.
 * @param region      AWS region where the job was submitted.
 */
case class EmrServerlessRun(
  jobRunId: String,
  sparkVersion: String,
  appSummary: Option[ApplicationSummary],
  region: String = DefaultRegion) extends JobRun {

  override val name: String = "Emr Serverless"

  override val documentationUrl: String = LinkEmrServerlessDocumentation

  private val applicationId = appSummary.map(_.id).getOrElse(NotAvailable)

  override def baseReleaseUrl(version: String): String = {
    val cleanedRelease = version.replaceAll("\\.", "").replaceAll("emr-", "")
    s"""https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-version-$cleanedRelease.html"""
  }

  override def deploymentInfo: String = s"$deployment / $applicationId / $jobRunId"

  override def isRunningOnEmr: Boolean = true

  override def release: List[String] = appSummary.map(s => List(s.releaseLabel())).getOrElse {
    AwsEmr.findReleaseBySparkVersion(sparkVersion, Region.of(region))
  }
}

/**
 * Represents an unknown or unsupported Spark deployment environment.
 * This is the fallback when the analyzer cannot detect EMR or other known modes.
 */
case class NotDetectedRun() extends JobRun {

  override val name: String = NotAvailable

  override val documentationUrl: String = ""

  override def baseReleaseUrl(version: String): String = ""

  override def deployment: String = NotAvailable

  override def deploymentInfo: String = NotAvailable

  override def isRunningOnEmr: Boolean = false

  override def release: List[String] = Nil

  override def releaseInfo(): String = NotAvailable
}