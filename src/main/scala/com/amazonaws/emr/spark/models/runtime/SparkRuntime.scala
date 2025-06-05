package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.spark.models.{AppContext, AppInfo}
import com.amazonaws.emr.utils.Formatter._
import org.apache.logging.log4j.scala.Logging

/**
 * SparkRuntime describes the resource configuration and runtime duration
 * of a Spark application, including both driver and executor specifications.
 *
 * This model is used in simulation, reporting, optimization, and configuration
 * generation. It provides utilities to convert runtime metadata into:
 *   - Human-readable summaries
 *   - `spark-submit` CLI strings
 *   - EMR classification JSON for deployment
 *   - Cost estimation models
 *
 * @param runtime                  Application wall-clock runtime in milliseconds.
 * @param driverCores              Number of CPU cores assigned to the driver.
 * @param driverMemory             Driver memory in bytes.
 * @param executorCores            Number of CPU cores per executor.
 * @param executorMemory           Executor memory in bytes.
 * @param executorStorageRequired  Disk storage required per executor (bytes).
 * @param executorsNum             Number of executors used.
 * @param sparkConf                Optional Spark configuration overrides.
 */
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

  /** Converts raw memory bytes to Spark-friendly format ("g" or "M"). */
  private def formatMemory(memory: Long): String = {
    if (memory >= byteStringAsBytes("1g")) s"${toGB(memory)}g"
    else s"${toMB(memory)}M"
  }

  private val driverMemStr = formatMemory(driverMemory)
  private val executorMemStr = formatMemory(executorMemory)

  /** Pretty print the runtime summary for display in logs or reports. */
  override def toString: String =
    s"""|App duration  : ${printDuration(runtime)}
        |Driver cores  : $driverCores
        |Driver memory : ${humanReadableBytes(driverMemory)}
        |Exec. cores   : $executorCores
        |Exec. memory  : ${humanReadableBytes(executorMemory)}
        |Exec. number  : $executorsNum
        |Exec. storage : ${humanReadableBytes(executorStorageRequired)}
        |""".stripMargin

  /** Converts the configuration to EMR `spark-defaults` classification JSON. */
  def emrOnEc2Classification: String = {
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

  /** Generates a space-delimited --conf string for CLI use. */
  def configurationStr: String =
    s"""|--conf spark.driver.cores=$driverCores --conf spark.driver.memory=$driverMemStr
        |--conf spark.executor.cores=$executorCores --conf spark.executor.memory=$executorMemStr
        |--conf spark.dynamicAllocation.maxExecutors=$executorsNum""".stripMargin

  /** Returns the total application runtime in hours (with optional padding). */
  def runtimeHrs(extraTimeMs: Long = 0L): Double = (runtime + extraTimeMs) / (3600 * 1000.0)

  /** Converts the driver configuration into a generic container request. */
  def driverRequest: ContainerRequest =
    ContainerRequest(1, driverCores, driverMemory, 0L)

  /** Converts the executor configuration into a generic container request. */
  def executorsRequest: ContainerRequest =
    ContainerRequest(executorsNum, executorCores, executorMemory, executorStorageRequired)
}

object SparkRuntime extends Logging {

  /**
   * Returns an empty default runtime configuration.
   */
  def empty: SparkRuntime = SparkRuntime(0, 0, 0, 0, 0, 0, 0)

  /**
   * Constructs a SparkRuntime from a parsed AppContext.
   *
   * Extracts runtime metrics, executor counts, and memory/core settings
   * from the observed application execution.
   *
   * @param appContext The parsed application context.
   * @return A populated SparkRuntime instance.
   */
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

  /**
   * Applies memory overhead (default 10%) to account for Spark JVM tuning.
   *
   * @param memory Base memory value.
   * @param defaultOverheadFactor Overhead percentage (default: 10%).
   * @return Adjusted memory with overhead applied.
   */
  def getMemoryWithOverhead(memory: Long, defaultOverheadFactor: Double = 0.1): Long = {
    (memory * (1 + defaultOverheadFactor)).toLong
  }

}