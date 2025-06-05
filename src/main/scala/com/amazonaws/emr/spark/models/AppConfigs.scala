package com.amazonaws.emr.spark.models

import com.amazonaws.emr.utils.Constants.NotAvailable
import com.amazonaws.emr.utils.Formatter

import scala.collection.mutable

/**
 * AppConfigs stores categorized configuration values extracted from a Spark event logs.
 *
 * It separates and maintains the following types of configurations:
 *   - `sparkConfigs`: All `spark.*` properties (e.g., memory, cores, scheduling)
 *   - `javaConfigs`: Java system properties (e.g., JVM args, GC options)
 *   - `hadoopConfigs`: Hadoop-related properties (e.g., `fs.s3a.*`, `mapreduce.*`)
 *   - `systemConfigs`: Environment variables or low-level system flags (e.g., classpath, `sun.java.command`)
 *
 * This class also exposes convenience accessors for:
 *   - Driver and executor cores
 *   - Driver and executor memory (converted from strings like "4g" to bytes)
 *   - Spark version (if known, else `"n/a"`)
 *
 */
class AppConfigs {

  var sparkVersion: String = NotAvailable

  val javaConfigs = new mutable.HashMap[String, String]()
  val sparkConfigs = new mutable.HashMap[String, String]()
  val hadoopConfigs = new mutable.HashMap[String, String]()
  val systemConfigs = new mutable.HashMap[String, String]()

  def driverCores: Int = sparkConfigs.getOrElse("spark.driver.cores", "1").toInt

  def driverMemory: Long = Formatter.byteStringAsBytes(sparkConfigs.getOrElse("spark.driver.memory", "1g"))

  def executorCores: Int = sparkConfigs.getOrElse("spark.executor.cores", "1").toInt

  def executorMemory: Long = Formatter.byteStringAsBytes(sparkConfigs.getOrElse("spark.executor.memory", "1g"))

}



