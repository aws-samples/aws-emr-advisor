package com.amazonaws.emr.spark.models.timespan

/**
 * We look at the application as a sequence of timeSpans
 */
trait TimeSpan {

  var endTime: Long = 0L
  var startTime: Long = 0L

  def duration: Option[Long] = {
    if (isFinished) Some(endTime - startTime)
    else None
  }

  def setEndTime(time: Long): Unit = if (endTime == 0L) endTime = time

  def setStartTime(time: Long): Unit = if (startTime == 0L) startTime = time

  def isFinished: Boolean = (endTime != 0 && startTime != 0)

}
