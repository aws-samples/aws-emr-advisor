package com.amazonaws.emr.spark.scheduler

import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.timespan.{JobTimeSpan, StageTimeSpan}
import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
 * The responsibility of computing time needed to process a given application
 * with different number of executors is divided among two classes.
 * CompletionEstimator and PQParallelStageScheduler.
 *
 * CompletionEstimator is responsible for scheduling stages based on DAG
 * PQParallelStageScheduler is the task scheduler, which ensures that
 * we are not running more tasks than the total number of cores in the
 * system.
 */
object CompletionEstimator extends Logging {

  private case class EstimatorState(stagesData: Map[Int, (Array[Long], Seq[Int])], stagesMap: Map[Int, StageTimeSpan]) {
    val waitingStages = new mutable.TreeSet[Int]()
    val runnableStages = new mutable.TreeSet[Int]()
    val runningStages = new mutable.TreeSet[Int]()
  }

  private def scheduleStage(stageID: Int, estate: EstimatorState, scheduler: PQParallelStageScheduler): Unit = {
    val stageData = estate.stagesData.getOrElse(stageID, (Array.emptyIntArray, List.empty[Int]))
    if (stageData._1.length > 0) {
      if (stageData._2.isEmpty) {
        //no parents
        estate.runnableStages += stageID
      } else {
        // some parents
        val nonSkippedParents = stageData._2.count(parentStage => estate.stagesData.contains(parentStage))
        if (nonSkippedParents > 0) {
          estate.waitingStages += stageID
          stageData._2.foreach(parentStage => {
            scheduleStage(parentStage, estate, scheduler)
          })
        } else {
          estate.runnableStages += stageID
        }
      }
    } else {
      println(s"Skipped stage $stageID with 0 tasks")
    }
  }

  private def processStages(maxStageIDs: List[Int], estate: EstimatorState, scheduler: PQParallelStageScheduler): Long = {
    //In the worst case we need to push all stages to completion
    val MAX_COMPLETION_TRIES = estate.stagesData.size + 1
    var completionRetries = 0
    while (!maxStageIDs
      .map(scheduler.isStageComplete)
      .forall(_ == true)
      && (completionRetries < MAX_COMPLETION_TRIES)) {
      if (estate.runnableStages.nonEmpty) {
        val copyOfRunningStages = estate.runnableStages.clone()
        val eligibleStages = copyOfRunningStages.diff(estate.runningStages)
        if (eligibleStages.nonEmpty) {
          val currentStageID = eligibleStages.toList.head
          estate.runningStages += currentStageID
          estate.runnableStages -= currentStageID
          estate.stagesData.getOrElse(currentStageID, (Array.emptyLongArray, List.empty[Int]))
            ._1.foreach(taskTime => {
            if (taskTime <= 0L) {
              //force each task to be at least 1 milli second
              //scheduler doesn't work with 0 or negative millis second tasks
              scheduler.schedule(1L, currentStageID)
            } else {
              scheduler.schedule(taskTime, currentStageID)
            }
          })
        }
      } else {
        // we have dependency to finish first
        scheduler.runTillStageCompletion()
        completionRetries += 1
      }
    }
    if (completionRetries >= MAX_COMPLETION_TRIES) {
      logInfo(s"ERROR: Estimation of job completion time aborted $completionRetries")
      logInfo(s"runnableStages ${estate.runnableStages}")
      logInfo(s"runningStages ${estate.runningStages}")
      logInfo(s"waitingStages ${estate.waitingStages}")
      logInfo(s"maxStageIDs $maxStageIDs")
      0L
    } else {
      scheduler.wallClockTime()
    }
  }

  /**
   * Many times multiple jobs are scheduled at the same time. We can identify them from the same sql execution id. This
   * method simulates scheduling of these jobs which can run concurrently.
   *
   * @param jobTimeSpans
   * @param executorCount
   * @param perExecutorCores
   * @return
   */
  private def estimateJobListWallClockTime(jobTimeSpans: List[JobTimeSpan], executorCount: Int, perExecutorCores: Int): Long = {
    val realJobTimeSpans = jobTimeSpans.filter(x => x.stageMap.nonEmpty)
    //Job has no stages and no tasks?
    //we assume that such a job will run in same time irrespective of number of cores
    var timeForEmptyJobs: Long = 0
    jobTimeSpans.filter(x => x.stageMap.isEmpty)
      .foreach(x => {
        if (x.duration.isDefined) {
          timeForEmptyJobs += x.duration.get
        }
      })

    if (realJobTimeSpans.isEmpty) {
      return timeForEmptyJobs
    }

    //Here we combine the data from all the parallel jobs. Scheduler takes the information about stages, time spent in
    //each task and the dependency between stages. This information is added to one data structure, but instead of
    //coming from just one job, we give data from all the parallel jobs
    val data = realJobTimeSpans.flatMap(x => x.stageMap.map(x => (x._1, (x._2.taskExecutionTimes, x._2.parentStageIDs)))).toMap
    val stagesMap = realJobTimeSpans.flatMap(x => x.stageMap).toMap
    val taskCountMap = new mutable.HashMap[Int, Int]()
    data.foreach(x => {
      taskCountMap(x._1) = x._2._1.length
    })

    val estate = EstimatorState(data, stagesMap)
    val scheduler = new PQParallelStageScheduler(executorCount * perExecutorCores, taskCountMap) {
      override def onStageFinished(stageID: Int): Unit = {
        estate.runningStages -= stageID
        val nowRunnableStages = estate.waitingStages.filter(eachStage => {
          estate.stagesData(eachStage)._2.forall(parentStage => isStageComplete(parentStage))
        })
        estate.waitingStages --= nowRunnableStages
        estate.runnableStages ++= nowRunnableStages
      }
    }
    //Here we find out the runnable stages to seed the simulation. scheduleStage takes the maxStageID of each job and
    //finds the list of runnable stages by traversing the dependency graph. We need to do this for all the jobs, since
    //multiple stages from different jobs can be runnable, given the parallel nature of jobs
    realJobTimeSpans.map(x => x.stageMap.keys.max).foreach(maxStageID => {
      scheduleStage(maxStageID, estate, scheduler)
    })

    //TODO: we might need to revisit this. Not sure if we should work with a single maxStageID now or switch to
    //list of maxStageIDs, one each for each parallel job.
    val maxStageIDs = realJobTimeSpans.map(x => x.stageMap.keys.max)
    processStages(maxStageIDs, estate, scheduler)
  }

  /**
   * New simulation method. The way we compute time spent in driver is now a bit different. Instead of just
   * summing up time spent in jobs and subtracting it from total application time, we now take into
   * account job level parallelism as defined by sql.execution.id. The earlier method would sometime
   * compute driver time as negative when sum of job times exceeded total application time.
   *
   * Well the logic is a bit more complicated than just using the sql.execution.id. We see jobs with the
   * same sql.execution.id also having some sort of dependencies. These are not captured anywhere in the
   * event log data (or may be I am not aware of it). Nevertheless, what we really do to make the jobGroups
   * is to first group them by sql.execution.id and then split these groups based on actual observed
   * parallelism. We look at jobs within a group sorted by start time, looking at pair at a time. If the
   * pair has some overlap in time, we assume they run in parallel. If we see a clean split, we split the
   * group.  The code is here: JobOverlapHelper.makeJobLists
   *
   * @param ac              Application context
   * @param executorCount   Number of Spark executors to run the estimation
   * @param executorsCores  Number of cores per executor that determines parallelism
   * @param appRealDuration Real time in ms the application took to complete
   * @return
   */
  def estimateAppWallClockTimeWithJobLists
  (ac: AppContext,
   executorCount: Int,
   executorsCores: Int,
   appRealDuration: Long): Long = {

    val appTotalTime = appRealDuration
    val jobGroupsList = JobOverlapHelper.makeJobLists(ac)

    // for each group we take the processing time as min of start time and max of end time from all the
    // jobs that are part of the group
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(ac)
    val driverTimeJobBased = appTotalTime - jobTime

    jobGroupsList.map(x => estimateJobListWallClockTime(x, executorCount, executorsCores)).sum + driverTimeJobBased

  }

}


