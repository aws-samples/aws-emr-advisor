package com.amazonaws.emr.api

import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import com.amazonaws.emr.Config.S3PreSignedUrlValidity
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest, S3Exception}
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest

import java.io.File
import java.time.Duration

/**
 * AwsUtils provides utility functions for interacting with AWS services,
 * especially Amazon S3, and provides helpers methods in the AWS context.
 */
object AwsUtils extends Logging {

  /**
   * Uploads a local file to the specified Amazon S3 bucket and key.
   *
   * This method uses the default AWS credentials chain to authenticate
   * and uploads the file using a synchronous S3 client.
   *
   * @param bucketName The name of the destination S3 bucket.
   * @param objectKey  The key (path) under which to store the object.
   * @param objectPath The path to the local file to upload.
   */
  def putS3Object(bucketName: String, objectKey: String, objectPath: String): Unit = {
    val credentialsProvider = DefaultCredentialsProvider.create
    val client = S3Client.builder.credentialsProvider(credentialsProvider).build

    try {
      val putOb = PutObjectRequest.builder.bucket(bucketName).key(objectKey).build
      client.putObject(putOb, RequestBody.fromFile(new File(objectPath)))
    } catch {
      case e: S3Exception => logger.error(e.getMessage)
    }
  }

  /**
   * Generates a pre-signed URL for downloading an object from S3.
   *
   * The returned URL is valid for a limited duration and allows
   * clients to download the object without needing AWS credentials.
   *
   * @param bucketName Name of the S3 bucket containing the object.
   * @param objectKey  S3 key (path) of the object.
   * @param validity   Validity duration of the URL in minutes.
   * @return A URL string that can be used to download the object.
   */
  def getS3ObjectPreSigned(bucketName: String, objectKey: String, validity: Int = S3PreSignedUrlValidity): String = {
    val credentialsProvider = DefaultCredentialsProvider.create
    try {
      val getOb = GetObjectRequest.builder.bucket(bucketName).key(objectKey).build
      val getPresReq = GetObjectPresignRequest.builder
        .signatureDuration(Duration.ofMinutes(validity))
        .getObjectRequest(getOb).build
      val preSigner = S3Presigner.builder.credentialsProvider(credentialsProvider).build
      val preSignedGetObjectRequest = preSigner.presignGetObject(getPresReq)
      preSignedGetObjectRequest.url.toString
    } catch {
      case e: S3Exception =>
        logger.error(e.getMessage)
        ""
    }
  }

  /**
   * Checks whether valid AWS credentials are configured and accessible.
   *
   * This method is intended as a placeholder to also include permission checks
   * for services such as S3, EMR, and EC2 in the future.
   * TODO - Implement IAM checks
   *
   * @return True if credentials are present (validity only), false otherwise.
   */
  def checkCredentialsAndPermissions(): Boolean = {
    hasValidCredentials
  }

  /**
   * Verifies that AWS credentials are available in the default provider chain.
   *
   * This method is used to ensure that the environment is correctly configured
   * for making AWS SDK calls, and it logs errors if no valid credentials are found.
   *
   * @return True if valid credentials are available, false otherwise.
   */
  private def hasValidCredentials: Boolean = {
    val credentialsProviderChain: AWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain()
    try {
      val awsCredentials = credentialsProviderChain.getCredentials
      logger.debug(s"Found valid AWS credentials")
      true
    } catch {
      case _: Exception =>
        logger.error("Error: Unable to load AWS credentials from any provider in the chain")
        false
    }
  }

}
