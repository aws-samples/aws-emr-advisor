package com.amazonaws.emr.api

import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import com.amazonaws.emr.Config.S3PreSignedUrlValidity
import org.apache.spark.internal.Logging
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest, S3Exception}
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest

import java.io.File
import java.time.Duration

object AwsUtils extends Logging {

  /**
   * Store a file on an Amazon S3 bucket.
   *
   * @param bucketName name of the bucket
   * @param objectKey  object prefix path
   * @param objectPath file path on the local filesystem
   */
  def putS3Object(bucketName: String, objectKey: String, objectPath: String): Unit = {
    val credentialsProvider = ProfileCredentialsProvider.create
    val client = S3Client.builder.credentialsProvider(credentialsProvider).build

    try {
      val putOb = PutObjectRequest.builder.bucket(bucketName).key(objectKey).build
      client.putObject(putOb, RequestBody.fromFile(new File(objectPath)))
    } catch {
      case e: S3Exception => logError(e.getMessage)
    }
  }

  /**
   * Return a Pre-Signed URL of an object stored in a S3 bucket.
   *
   * @param bucketName name of the bucket
   * @param objectKey  object prefix path
   * @param validity   validity of the url expressed in minutes
   * @return pre-signed url string
   */
  def getS3ObjectPreSigned(bucketName: String, objectKey: String, validity: Int = S3PreSignedUrlValidity): String = {
    val credentialsProvider = ProfileCredentialsProvider.create
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
        logError(e.getMessage)
        ""
    }
  }

  private def hasValidCredentials: Boolean = {
    val credentialsProviderChain: AWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain()
    try {
      val awsCredentials = credentialsProviderChain.getCredentials
      println(s"Found valid AWS credentials: ${awsCredentials.getAWSAccessKeyId}")
      true
    } catch {
      case _: Exception =>
        println("Error: Unable to load AWS credentials from any provider in the chain")
        false
    }
  }

  /**
   * TODO - Verify if AWS Credentials exists and the there are all the required Service permissions
   */
  def checkCredentialsAndPermissions(): Boolean = {
    hasValidCredentials
  }

}
