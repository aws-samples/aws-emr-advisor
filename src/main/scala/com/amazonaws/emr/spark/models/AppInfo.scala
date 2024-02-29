package com.amazonaws.emr.spark.models

import com.amazonaws.emr.utils.Constants.NotAvailable
import com.amazonaws.emr.spark.models.runtime.{JobRun, NotDetectedRun}
import org.apache.spark.utils.SparkHelper.SparkCommand

class AppInfo {

  var runtime: JobRun = NotDetectedRun()

  var applicationID: String = NotAvailable
  var sparkAppName: String = NotAvailable
  var sparkVersion: String = NotAvailable
  var startTime: Long = 0L
  var endTime: Long = 0L

  val insightsInfo = new scala.collection.mutable.HashMap[String, String]
  val insightsWarn = new scala.collection.mutable.HashMap[String, String]
  val insightsIssue = new scala.collection.mutable.HashMap[String, String]

  val insightsTaskFailures = new scala.collection.mutable.HashMap[Long, String]

  var sparkCmd: Option[SparkCommand] = None

  /** Return the time (in ms) the application was running */
  def duration: Long = endTime - startTime

}