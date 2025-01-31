package org.apache.spark.utils

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.scheduler.ReplayListenerBus.ReplayEventsFilter
import org.apache.spark.util.JsonProtocol

/**
 * Custom `ReplayListenerBus` class to parallelize EventLog processing using streams
 */
class CustomReplayListenerBus extends ReplayListenerBus {

  def replay(line: String, eventsFilter: ReplayEventsFilter): Boolean = {
    try {
      if (eventsFilter(line)) postToAll(JsonProtocol.sparkEventFromJson(line))
    } catch {
      case _: ClassNotFoundException =>
      //logDebug(s"Drop incompatible event log: $line")
      case _: UnrecognizedPropertyException =>
      //logDebug(s"Drop incompatible event log: $line")
      case _: JsonParseException =>
      //logWarning(s"Got JsonParseException log: $line")
      case t: Throwable =>
        logWarning(s"Ignore error at: $line, ${t.getMessage}")
    }
    true
  }
}
