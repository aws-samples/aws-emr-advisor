package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.spark.models.{AppContext, AppInfo}
import com.amazonaws.emr.utils.Formatter._
import org.apache.logging.log4j.scala.Logging

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
    s"""
       |App duration  : ${printDuration(runtime)}
       |Driver cores  : $driverCores
       |Driver memory : ${humanReadableBytes(driverMemory)}
       |Exec. cores   : $executorCores
       |Exec. memory  : ${humanReadableBytes(executorMemory)}
       |Exec. number  : $executorsNum
       |Exec. storage : ${humanReadableBytes(executorStorageRequired)}
       |""".stripMargin

  def toHtml: String =
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
       |    "spark.dynamicAllocation.maxExecutors": "$executorsNum"$sparkConfigs
       |  }
       |}""".stripMargin
  }

  def asEmrClassification: String = {
    s"""[
       |${getSparkClassification.split("\n").mkString("  ", "\n  ", "")}
       |]""".stripMargin
  }

  def sparkMainConfString = s"""--conf spark.driver.cores=$driverCores --conf spark.driver.memory=$driverMemStr --conf spark.executor.cores=$executorCores --conf spark.executor.memory=$executorMemStr --conf spark.dynamicAllocation.maxExecutors=$executorsNum"""

  def runtimeHrs(extraTimeMs: Long = 0L): Double = (runtime + extraTimeMs) / (3600 * 1000.0)

  def getDriverContainer: ContainerRequest = {
    ContainerRequest(1, driverCores, driverMemory, 0L)
  }

  def getExecutorContainer: ContainerRequest = {
    ContainerRequest(
      executorsNum,
      executorCores,
      executorMemory,
      executorStorageRequired
    )
  }

}

object SparkRuntime extends Logging {

  def empty: SparkRuntime = SparkRuntime(0, 0, 0, 0, 0, 0, 0)

  def getMemoryWithOverhead(memory: Long, defaultOverheadFactor: Double = 0.1): Long = {
    (memory * (1 + defaultOverheadFactor)).toLong
  }

  def fromAppContext(appContext: AppContext): SparkRuntime = {
    logger.info("Analyze Spark settings...")
    val currentConf = SparkRuntime(
      appContext.appInfo.duration,
      appContext.appSparkExecutors.defaultDriverCores,
      appContext.appSparkExecutors.defaultDriverMemory,
      appContext.appSparkExecutors.defaultExecutorCores,
      appContext.appSparkExecutors.defaultExecutorMemory,
      appContext.appSparkExecutors.getRequiredStoragePerExecutor,
      appContext.appSparkExecutors.executorsMaxRunning
    )
    logger.debug(currentConf)
    currentConf
  }

}