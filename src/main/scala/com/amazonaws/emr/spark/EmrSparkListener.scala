package com.amazonaws.emr.spark

import com.amazonaws.emr.spark.models.{AppConfigs, AppContext, AppInfo, AppMetrics}
import com.amazonaws.emr.spark.models.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.scheduler._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

class EmrSparkListener extends SparkListener with Logging {

  private val appInfo = new AppInfo
  private val appConfigs = new AppConfigs
  private val appMetrics = new AppMetrics

  private val executorMap = new ConcurrentHashMap[String, ExecutorTimeSpan].asScala
  private val hostMap = new ConcurrentHashMap[String, HostTimeSpan].asScala
  private val jobMap = new ConcurrentHashMap[Long, JobTimeSpan].asScala
  private val jobSQLExecIDMap = new ConcurrentHashMap[Long, Long].asScala
  private val stageMap = new ConcurrentHashMap[Int, StageTimeSpan].asScala
  private val stageIDToJobID = new ConcurrentHashMap[Int, Long].asScala

  // used to keep track of the runtime for incomplete applications
  private var lastEventTimestamp = 0L

  def finalUpdate(): AppContext = {
    // Update termination time for all executors still alive
    executorMap.keys.foreach { x =>
      if (executorMap(x).endTime == 0) executorMap(x).setEndTime(appInfo.endTime)
    }

    // Set end times for the jobs for which onJobEnd event was missed
    jobMap.foreach(x => {
      if (jobMap(x._1).endTime == 0) {
        //Lots of computations go wrong if we don't have
        //application end time
        //set it to end time of the stage that finished last
        if (x._2.stageMap.nonEmpty) {
          jobMap(x._1).setEndTime(x._2.stageMap.map(y => y._2.endTime).max)
        } else {
          //no stages? set it to endTime of the app
          jobMap(x._1).setEndTime(appInfo.endTime)
        }
      }
    })

    // Aggregate Spark Job Metrics
    stageIDToJobID
      .filter(s => stageMap.contains(s._1))
      .foreach { case (stageId, jobId) =>
        try {
          val jobTimeSpan = jobMap(jobId)
          val stageTimeSpan = stageMap(stageId)
          jobTimeSpan.updateAggregateTaskMetrics(stageTimeSpan.stageMetrics)
        } catch {
          case e: Throwable =>
            logger.warn(s"Can't finalize stageId: $stageId")
            e.printStackTrace()
        }
      }

    stageIDToJobID
      .filter(s => stageMap.contains(s._1))
      .foreach { case (stageId, jobId) =>
        try {
          val jobTimeSpan = jobMap(jobId)
          val stageTimeSpan = stageMap(stageId)
          jobTimeSpan.addStage(stageTimeSpan)
          stageTimeSpan.finalUpdate()
        } catch {
          case e: Throwable =>
            logger.warn(s"Can't finalize stageId: $stageId")
            e.printStackTrace()
        }
      }

    val appContext = new AppContext(
      appInfo,
      appConfigs,
      appMetrics,
      hostMap.toMap,
      executorMap.toMap,
      jobMap.toMap,
      jobSQLExecIDMap.toMap,
      stageMap.toMap,
      stageIDToJobID.toMap)

    // check if the application terminated, otherwise we set the time
    // of the last collected event
    if (appInfo.endTime == 0L) appInfo.endTime = lastEventTimestamp

    appContext
  }

  //=========================================================================================
  // Application
  //=========================================================================================
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    updateLastEvent(applicationStart.time)
    appInfo.applicationID = applicationStart.appId.getOrElse("NA")
    appInfo.startTime = applicationStart.time
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    updateLastEvent(applicationEnd.time)
    appInfo.endTime = applicationEnd.time
  }

  //=========================================================================================
  // Executors
  //=========================================================================================
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    updateLastEvent(blockManagerAdded.time)

    val executorTimeSpan = getOrCreateExecutorTimeSpan(blockManagerAdded.blockManagerId.executorId)
    executorTimeSpan.hostID = blockManagerAdded.blockManagerId.host
    executorTimeSpan.memoryMax = blockManagerAdded.maxMem
    executorTimeSpan.memoryMaxOnHeap = blockManagerAdded.maxOnHeapMem.getOrElse(0L)
    executorTimeSpan.memoryMaxOffHeap = blockManagerAdded.maxOffHeapMem.getOrElse(0L)

    // update driver start time
    if (blockManagerAdded.blockManagerId.executorId.equals("driver")) {
      executorTimeSpan.setStartTime(blockManagerAdded.time)
    }

  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    updateLastEvent(executorAdded.time)

    val executorTimeSpan = getOrCreateExecutorTimeSpan(executorAdded.executorId)
    executorTimeSpan.hostID = executorAdded.executorInfo.executorHost
    executorTimeSpan.executorInfo = executorAdded.executorInfo
    executorTimeSpan.cores = executorAdded.executorInfo.totalCores
    executorTimeSpan.setStartTime(executorAdded.time)

    val hostTimeSpan = getOrCreateHostTimeSpan(executorAdded.executorInfo.executorHost)
    hostTimeSpan.setStartTime(executorAdded.time)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    updateLastEvent(executorRemoved.time)

    val executorTimeSpan = getOrCreateExecutorTimeSpan(executorRemoved.executorId)
    executorTimeSpan.setEndTime(executorRemoved.time)

  }

  //=========================================================================================
  // Job / Action
  //=========================================================================================
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    updateLastEvent(jobStart.time)

    val jobTimeSpan = getOrCreateJobTimeSpan(jobStart.jobId)
    jobTimeSpan.setStartTime(jobStart.time)
    jobStart.stageIds.foreach(stageID => {
      stageIDToJobID(stageID) = jobStart.jobId
    })

    val sqlExecutionID = jobStart.properties.getProperty("spark.sql.execution.id")
    if (sqlExecutionID != null && sqlExecutionID.nonEmpty) {
      jobSQLExecIDMap(jobStart.jobId) = sqlExecutionID.toLong
    }

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    updateLastEvent(jobEnd.time)

    val jobTimeSpan = getOrCreateJobTimeSpan(jobEnd.jobId)
    jobTimeSpan.setEndTime(jobEnd.time)
    jobEnd.jobResult match {
      case JobSucceeded => appMetrics.addJob()
      case _ => appMetrics.addFailedJob()
    }

  }

  //=========================================================================================
  // Stage
  //=========================================================================================
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    val stageTimeSpan = getOrCreateStageTimeSpan(stageInfo.stageId)

    if (stageInfo.submissionTime.isDefined) {
      stageTimeSpan.setStartTime(stageInfo.submissionTime.get)
      updateLastEvent(stageInfo.submissionTime.get)
    }

    stageTimeSpan.numberOfTasks = stageInfo.numTasks
    stageTimeSpan.setParentStageIDs(stageInfo.parentIds)

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageTimeSpan = getOrCreateStageTimeSpan(stageInfo.stageId)

    appMetrics.addStage()

    if (stageInfo.submissionTime.isDefined) stageTimeSpan.setStartTime(stageInfo.submissionTime.get)
    if (stageInfo.completionTime.isDefined) stageTimeSpan.setEndTime(stageInfo.completionTime.get)

    // track failed stage error messages
    if (stageInfo.failureReason.isDefined) appMetrics.addFailedStage()

    stageTimeSpan.numberOfTasks = stageInfo.numTasks
    stageTimeSpan.setParentStageIDs(stageInfo.parentIds)
  }

  //=========================================================================================
  // Task
  //=========================================================================================
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    updateLastEvent(taskEnd.taskInfo.finishTime)

    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val executorMetrics = taskEnd.taskExecutorMetrics

    if (taskMetrics == null) return

    // update app metrics
    appMetrics.appAggMetrics.update(taskMetrics, taskInfo)

    // update executor metrics
    val executorTimeSpan = getOrCreateExecutorTimeSpan(taskInfo.executorId)
    executorTimeSpan.updateMetrics(taskMetrics, taskInfo)
    executorTimeSpan.updateMetrics(executorMetrics)

    // update host metrics
    val hostTimeSpan = getOrCreateHostTimeSpan(taskInfo.host)
    hostTimeSpan.updateMetrics(taskMetrics, taskInfo)

    // update stage metrics
    val stageTimeSpan = getOrCreateStageTimeSpan(taskEnd.stageId)
    stageTimeSpan.updateAggregateTaskMetrics(taskMetrics, taskInfo)
    stageTimeSpan.updateTasks(taskInfo, taskMetrics)

    appMetrics.addTask()
    if (taskEnd.taskInfo.failed) {
      appMetrics.addFailedTask()
      appInfo.insightsTaskFailures(taskInfo.taskId) = taskEnd.reason.toString
    } else if (taskEnd.taskInfo.killed) appMetrics.addKilledTask()

  }

  //=========================================================================================
  // Other
  //=========================================================================================
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    // skip classpath entries
    val configs = environmentUpdate.environmentDetails
    configs("Spark Properties").foreach(x => appConfigs.sparkConfigs.put(x._1, x._2))
    configs("Hadoop Properties").foreach(x => appConfigs.hadoopConfigs.put(x._1, x._2))
    configs("System Properties").foreach(x => appConfigs.systemConfigs.put(x._1, x._2))
    configs("JVM Information").foreach(x => appConfigs.javaConfigs.put(x._1, x._2))

    appInfo.sparkAppName = configs("Spark Properties")
      .filter(_._1.equals("spark.app.name"))
      .map(_._2)
      .headOption
      .getOrElse("NA")
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case SparkListenerLogStart(sparkVersion) =>
        appInfo.sparkVersion = sparkVersion
        appConfigs.sparkVersion = sparkVersion
    }
  }

  /** Used to keep track of the last event parsed in the eventLogs */
  private def updateLastEvent(time: Long): Unit = lastEventTimestamp = {
    if (lastEventTimestamp >= time) lastEventTimestamp
    else time
  }

  private def getOrCreateExecutorTimeSpan(executorId: String): ExecutorTimeSpan = synchronized {
    if (executorMap.contains(executorId)) executorMap(executorId)
    else {
      executorMap(executorId) = new ExecutorTimeSpan(executorId)
      executorMap(executorId)
    }
  }

  private def getOrCreateHostTimeSpan(hostId: String): HostTimeSpan = synchronized {
    if (hostMap.contains(hostId)) hostMap(hostId)
    else {
      hostMap(hostId) = new HostTimeSpan(hostId)
      hostMap(hostId)
    }
  }

  private def getOrCreateJobTimeSpan(jobId: Int): JobTimeSpan = synchronized {
    if (jobMap.contains(jobId)) jobMap(jobId)
    else {
      jobMap(jobId) = new JobTimeSpan(jobId)
      jobMap(jobId)
    }
  }

  private def getOrCreateStageTimeSpan(stageId: Int): StageTimeSpan = synchronized {
    if (stageMap.contains(stageId)) stageMap(stageId)
    else {
      stageMap(stageId) = new StageTimeSpan(stageId)
      stageMap(stageId)
    }
  }

}