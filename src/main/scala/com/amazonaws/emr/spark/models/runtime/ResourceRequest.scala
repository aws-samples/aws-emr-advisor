package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.utils.Formatter.humanReadableBytes

sealed trait ResourceRequest {
  val count: Int
  val cores: Int
  val memory: Long
  val storage: Long

  override def toString: String = {
    s"""Containers ($count): cores:$cores memory:${humanReadableBytes(memory)} storage:${humanReadableBytes(storage)}"""
  }

}

case class ContainerRequest(count: Int, cores: Int, memory: Long, storage: Long) extends ResourceRequest

case class K8sRequest(count: Int, cores: Int, memory: Long, storage: Long) extends ResourceRequest

case class YarnRequest(count: Int, cores: Int, memory: Long, storage: Long) extends ResourceRequest
