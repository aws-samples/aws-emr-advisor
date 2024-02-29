package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.utils.Formatter._

case class SparkRuntime(
  runtime: Long,
  driverCores: Int,
  driverMemory: Long,
  executorCores: Int,
  executorMemory: Long,
  executorStorageRequired: Long,
  executorsNum: Int,
  sparkConf: Map[String, String] = Map()
) {

  val driverMemStr = if (driverMemory >= byteStringAsBytes("1g")) s"${toGB(driverMemory)}g" else s"${toMB(driverMemory)}M"
  val executorMemStr = if (executorMemory >= byteStringAsBytes("1g")) s"${toGB(executorMemory)}g" else s"${toMB(executorMemory)}M"

  override def toString: String =
    s"""Your application was running for <b>${printDurationStr(runtime)}</b> using <b>$executorsNum</b>
       |executors each with <b>$executorCores</b> cores and <b>${humanReadableBytes(executorMemory)}</b> memory.
       |The driver was launched with <b>$driverCores</b> cores and <b>${humanReadableBytes(driverMemory)}</b> memory
       |""".stripMargin

  def toSparkSubmit(appInfo: AppInfo): String = {
    s"""spark-submit \\
       |  --driver-cores $driverCores \\
       |  --driver-memory $driverMemStr \\
       |  --executor-cores $executorCores \\
       |  --executor-memory $executorMemStr \\
       |  --num-executors $executorsNum \\
       |  ${sparkConf.map(x => s"--conf ${x._1}=${x._2} \\").mkString("", "\n  ", "")}...
       |""".stripMargin
  }

  def getSparkClassification: String = {
    val sparkConfigs = sparkConf.map(x => s""",\n      \"${x._1}\": \"${x._2}\"""").mkString
    s"""{
       |  "Classification": "spark-defaults",
       |  "Properties": {
       |    "spark.driver.cores": "$driverCores",
       |    "spark.driver.memory": "$driverMemStr",
       |    "spark.executor.cores": "$executorCores",
       |    "spark.executor.memory": "$executorMemStr",
       |    "spark.executor.instances": "$executorsNum",
       |    "spark.dynamicAllocation.maxExecutors": "$executorsNum"$sparkConfigs
       |  }
       |}""".stripMargin
  }

  def asEmrClassification: String = {
    s"""[
       |${getSparkClassification.split("\n").mkString("  ", "\n  ", "")}
       |]""".stripMargin
  }

  def sparkMainConfString = s"""--conf spark.driver.cores=$driverCores --conf spark.driver.memory=$driverMemStr --conf spark.executor.cores=$executorCores --conf spark.executor.memory=$executorMemStr --conf spark.executor.instances=$executorsNum --conf spark.dynamicAllocation.maxExecutors=$executorsNum"""

  def runtimeHrs(extraTimeMs: Long = 0L): Double = (runtime + extraTimeMs) / (3600 * 1000.0)

}

object SparkRuntime {

  def empty: SparkRuntime = SparkRuntime(0, 0, 0, 0, 0, 0, 0)

  def getMemoryWithOverhead(memory: Long, defaultOverheadFactor: Double = 0.1): Long = {
    (memory * (1 + defaultOverheadFactor)).toLong
  }

}