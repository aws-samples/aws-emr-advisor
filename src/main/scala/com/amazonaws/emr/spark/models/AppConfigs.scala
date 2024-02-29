package com.amazonaws.emr.spark.models

import com.amazonaws.emr.utils.Constants.NotAvailable
import com.amazonaws.emr.utils.Formatter

import scala.collection.mutable

class AppConfigs {

  var sparkVersion: String = NotAvailable

  val javaConfigs = new mutable.HashMap[String, String]()
  val sparkConfigs = new mutable.HashMap[String, String]()
  val hadoopConfigs = new mutable.HashMap[String, String]()
  val systemConfigs = new mutable.HashMap[String, String]()

  val defaultSparkMemoryStorageFraction: Float = sparkConfigs
    .getOrElse("spark.memory.storageFraction", "0.5")
    .toFloat

  val defaultSparkMemoryFraction: Float = sparkConfigs
    .getOrElse("spark.memory.fraction", "0.6")
    .toFloat

  val defaultYarnOverheadFactor: Float = sparkConfigs
    .getOrElse("spark.executor.memoryOverheadFactor", "0.1")
    .toFloat

  def driverCores: Int = sparkConfigs.getOrElse("spark.driver.cores", "1").toInt

  def driverMemory: Long = Formatter.byteStringAsBytes(sparkConfigs.getOrElse("spark.driver.memory", "1g"))

  def executorCores: Int = sparkConfigs.getOrElse("spark.executor.cores", "1").toInt

  def executorMemory: Long = Formatter.byteStringAsBytes(sparkConfigs.getOrElse("spark.executor.memory", "1g"))

}



