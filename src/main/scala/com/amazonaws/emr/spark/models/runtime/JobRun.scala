package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.utils.Constants.{LinkEmrOnEc2Documentation, LinkEmrOnEksDocumentation, LinkEmrServerlessDocumentation, NotAvailable}
import software.amazon.awssdk.services.emrserverless.model._

trait JobRun {

  val name: String

  val documentationUrl: String

  def baseReleaseUrl(version: String): String

  def isRunningOnEmr: Boolean

  def deployment: String = {
    if (documentationUrl.nonEmpty)
      s"""<a target="_blank" href="$documentationUrl">$name</a>"""
    else
      name
  }

  def deploymentInfo: String

  // we use a list of string because when we cannot determine the exact version of EMR, we'll try to infer it
  // using the spark version. However, multiple emr versions might use the same Spark release.
  def release: List[String]

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

case class EmrOnEc2Run(
  clusterId: String,
  sparkVersion: String,
  releaseLabel: Option[String],
  region: String = "") extends JobRun {

  override val name: String = "Emr On Ec2"

  override val documentationUrl: String = LinkEmrOnEc2Documentation

  private val clusterUrl = s"https://$region.console.aws.amazon.com/emr/home?region=$region#/clusterDetails/$clusterId"

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

  override def release: List[String] = {
    if (releaseLabel.nonEmpty) List(releaseLabel.get)
    else AwsEmr.findReleaseBySparkVersion(sparkVersion)
  }

  override def isRunningOnEmr: Boolean = true

}

case class EmrOnEksRun(
  sparkVersion: String,
  releaseLabel: Option[String],
  region: String = "") extends JobRun {

  override val name: String = "Emr On Eks"

  override val documentationUrl: String = LinkEmrOnEksDocumentation


  override def baseReleaseUrl(version: String): String = {
    val cleanedRelease = version.replaceAll("emr-", "emr-eks-")
    s"""https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/$cleanedRelease.html"""
  }

  override def deploymentInfo: String = s"$deployment"

  override def release: List[String] = {
    if (releaseLabel.nonEmpty) List(releaseLabel.get)
    else AwsEmr.findReleaseBySparkVersion(sparkVersion).filter(_.contains(".0"))
  }

  override def isRunningOnEmr: Boolean = true

}

case class EmrServerlessRun(
  jobRunId: String,
  sparkVersion: String,
  appSummary: Option[ApplicationSummary]) extends JobRun {

  override val name: String = "Emr Serverless"

  override val documentationUrl: String = LinkEmrServerlessDocumentation

  private val applicationId = appSummary.map(_.id).getOrElse(NotAvailable)

  override def baseReleaseUrl(version: String): String = {
    val cleanedRelease = version.replaceAll("\\.", "").replaceAll("emr-", "")
    s"""https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-version-$cleanedRelease.html"""
  }

  override def deploymentInfo: String = s"$deployment / $applicationId / $jobRunId"

  override def release: List[String] = appSummary.map(s => List(s.releaseLabel())).getOrElse {
    // Emr serverless only publish first version of a release in the doc (e.g 6.8.0, 6.9.0)
    // minor versions are not published so we skip them
    AwsEmr.findReleaseBySparkVersion(sparkVersion).filter(_.contains(".0"))
  }

  override def isRunningOnEmr: Boolean = true

}

case class NotDetectedRun() extends JobRun {

  override val name: String = NotAvailable

  override val documentationUrl: String = ""

  override def baseReleaseUrl(version: String): String = ???

  override def deployment: String = NotAvailable

  override def deploymentInfo: String = NotAvailable

  override def release: List[String] = Nil

  override def isRunningOnEmr: Boolean = false
}