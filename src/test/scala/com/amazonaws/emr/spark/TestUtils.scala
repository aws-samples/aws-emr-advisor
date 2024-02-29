package com.amazonaws.emr.spark

import com.amazonaws.emr.spark.models.AppContext

trait TestUtils {

  def parse(resourcePath: String): AppContext = {
    val eventLog = getClass.getResource(resourcePath).getPath
    val logParser = new EmrSparkLogParser(eventLog)
    logParser.process()
  }

}
