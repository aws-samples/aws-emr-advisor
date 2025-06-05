package org.apache.spark.utils

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.scheduler.ReplayListenerBus.ReplayEventsFilter
import org.apache.spark.util.JsonProtocol


/**
 * CustomReplayListenerBus extends Spark's `ReplayListenerBus` to provide
 * a simplified interface for replaying individual event log lines.
 *
 * This class is designed for streaming or parallelized processing of
 * Spark event logs, enabling the caller to filter and process each log
 * line individually without needing to load the entire log upfront.
 *
 * Common exceptions such as `JsonParseException`, `UnrecognizedPropertyException`,
 * and `ClassNotFoundException` are caught and ignored to maintain robustness
 * in cases where the logs contain unsupported or malformed events.
 *
 * This is especially useful for analyzing large-scale logs where partial
 * corruption or version mismatches are expected.
 */
class CustomReplayListenerBus extends ReplayListenerBus {

  /**
   * Attempts to parse and dispatch a single event log line to registered listeners.
   *
   * This method wraps the JSON parsing and event posting logic in a
   * try-catch block to tolerate failures in deserialization or class compatibility.
   *
   * @param line         A single line from the Spark event log (JSON format).
   * @param eventsFilter A predicate to determine whether this event should be processed.
   * @return Always returns `true`, indicating the replay attempt was made regardless of outcome.
   */
  def replay(line: String, eventsFilter: ReplayEventsFilter): Boolean = {
    try {
      if (eventsFilter(line)) postToAll(JsonProtocol.sparkEventFromJson(line))
    } catch {
      case _: ClassNotFoundException =>
      // Drop incompatible class-based events (e.g., custom classes not on classpath)
      case _: UnrecognizedPropertyException =>
      // Drop logs with unknown JSON fields not supported by current Spark version
      case _: JsonParseException =>
      // Drop logs with invalid JSON syntax
      case t: Throwable =>
        logWarning(s"Ignore error at: $line, ${t.getMessage}")
    }
    true
  }
}
