package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.utils.Formatter.humanReadableBytes

/**
 * A sealed trait representing a standardized resource request for a Spark role (driver or executor).
 *
 * This abstraction is used to describe resource allocation in a platform-agnostic way,
 * capturing the number of containers (or pods), cores, memory, and storage per container.
 *
 * Implementations include:
 *   - [[ContainerRequest]] (generic)
 *   - [[K8sRequest]]       (for Kubernetes-based Spark on EKS/Serverless)
 *   - [[YarnRequest]]      (for Hadoop YARN / EMR on EC2)
 *
 * @constructor Implementations must define values for count, cores, memory, and storage.
 */
sealed trait ResourceRequest {

  /** Number of containers/pods/executors requested. */
  val count: Int

  /** Number of CPU cores per container. */
  val cores: Int

  /** Amount of memory (in bytes) per container. */
  val memory: Long

  /** Amount of storage (in bytes) per container. */
  val storage: Long

  override def toString: String = {
    s"""Containers ($count): cores:$cores memory:${humanReadableBytes(memory)} storage:${humanReadableBytes(storage)}"""
  }

}

/**
 * Generic resource request, not bound to a specific execution platform.
 *
 * Useful as a base representation or for testing.
 *
 * @param count   Number of containers (e.g., executors).
 * @param cores   Number of cores per container.
 * @param memory  Memory per container in bytes.
 * @param storage Storage per container in bytes.
 */
case class ContainerRequest(count: Int, cores: Int, memory: Long, storage: Long) extends ResourceRequest

/**
 * Resource request specific to Kubernetes-based Spark environments (e.g., EMR on EKS, EMR Serverless).
 *
 * Used for mapping containerized requests to SparkApplication specs or pod templates.
 *
 * @param count   Number of executor pods.
 * @param cores   vCPU per pod.
 * @param memory  Memory per pod in bytes.
 * @param storage Storage per pod in bytes.
 */
case class K8sRequest(count: Int, cores: Int, memory: Long, storage: Long) extends ResourceRequest

/**
 * Resource request specific to YARN-based Spark environments (e.g., EMR on EC2).
 *
 * Used to map Spark executor specs to YARN container allocation.
 *
 * @param count   Number of containers.
 * @param cores   CPU cores per container.
 * @param memory  Memory per container in bytes.
 * @param storage Storage per container in bytes.
 */
case class YarnRequest(count: Int, cores: Int, memory: Long, storage: Long) extends ResourceRequest
