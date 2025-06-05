package com.amazonaws.emr.utils

import org.apache.spark.network.util.JavaUtils

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit._
import java.util.{Date, Locale}

/**
 * Formatter provides a collection of utility methods for formatting
 * time durations, byte sizes, and numeric values into human-readable
 * or Spark-compatible forms.
 *
 * This object is commonly used throughout Spark EMR reporting and optimization
 * tools to convert raw data (e.g., milliseconds, bytes, command-line args)
 * into consistent, interpretable formats for display and configuration.
 */
object Formatter {

  private val DEFAULT_DATE = new SimpleDateFormat("dd/MM/yyyy")
  private val DEFAULT_TIME = new SimpleDateFormat("HH:mm:ss")

  /** Returns a formatted date string from a millisecond timestamp (e.g., "27/05/2025"). */
  def printDate(time: Long): String = DEFAULT_DATE.format(new Date(time))

  /** Returns a formatted time string from a millisecond timestamp (e.g., "13:45:02"). */
  def printTime(time: Long): String = DEFAULT_TIME.format(new Date(time))

  /**
   * Parses a size string (e.g., "250m", "1g") and returns the size in bytes.
   * Defaults to bytes if no suffix is provided.
   *
   * @param str Human-readable size string.
   * @return Size in bytes.
   */
  def byteStringAsBytes(str: String): Long = JavaUtils.byteStringAsBytes(str)

  /**
   * Converts a byte count into a human-readable string (e.g., "2.3 GB").
   *
   * @param size Size in bytes.
   * @return Formatted string with appropriate unit (KB, MB, GB, TB).
   */
  def humanReadableBytes(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (Math.abs(size) >= 1 * TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (Math.abs(size) >= 1 * GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (Math.abs(size) >= 1 * MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else {
        (size.asInstanceOf[Double] / KB, "KB")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /** Converts milliseconds to whole days. */
  def toDays(millis: Long): Int = {
    val seconds = millis / 1000F
    val minutes = seconds / 60F
    val hours = minutes / 60F
    (hours / 24F).toInt
  }

  /**
   * Formats a duration from milliseconds into `HHh MMm SSs` format.
   *
   * @param millis Duration in milliseconds.
   */
  def printDuration(millis: Long): String = {
    val hours = if (MILLISECONDS.toHours(millis) > 0) f"${MILLISECONDS.toHours(millis)}%02dh" else ""
    val mins = MILLISECONDS.toMinutes(millis) - HOURS.toMinutes(MILLISECONDS.toHours(millis))
    val minutes = if (mins > 0) f"$mins%02dm" else ""
    val seconds = f"${MILLISECONDS.toSeconds(millis) - MINUTES.toSeconds(MILLISECONDS.toMinutes(millis))}%02ds"
    s"""$hours $minutes $seconds"""
  }

  /**
   * Formats a duration from milliseconds into a natural language string.
   *
   * @param millis Duration in milliseconds.
   * @return e.g., "2 hours 5 minutes and 30 seconds"
   */
  def printDurationStr(millis: Long): String = {
    val hours = if (MILLISECONDS.toHours(millis) > 0) s"${MILLISECONDS.toHours(millis)} hours" else ""
    val mins = MILLISECONDS.toMinutes(millis) - HOURS.toMinutes(MILLISECONDS.toHours(millis))
    val minutes = if (mins > 0) s"$mins minutes and" else ""
    val seconds = s"${MILLISECONDS.toSeconds(millis) - MINUTES.toSeconds(MILLISECONDS.toMinutes(millis))} seconds"
    s"""$hours $minutes $seconds"""
  }

  /** Rounds a floating point number up to the nearest integer. */
  def roundUp(d: Double): Int = math.ceil(d).toInt

  /** Formats a number with locale-specific digit grouping (e.g., 1,000,000). */
  def printNumber(number: Number): String = java.text.NumberFormat.getIntegerInstance.format(number)

  /**
   * Formats a duration (in ms) into HHh MMm (used in summaries).
   *
   * @param millis Time in milliseconds.
   */
  def pcm(millis: Long): String = {
    val millisForMinutes = millis % (60 * 60 * 1000)

    "%02dh %02dm".format(
      MILLISECONDS.toHours(millis),
      MILLISECONDS.toMinutes(millisForMinutes))
  }

  /**
   * Formats a time value in nanoseconds to a human-readable duration string.
   *
   * @param ns Time in nanoseconds.
   * @return Formatted string (e.g., "12.0 ms", "1.5 s").
   */
  def formatNanoSeconds(ns: Long): String = {
    val MS = 1000000L
    val SEC = 1000 * MS
    val MT = 60 * SEC
    val HR = 60 * MT

    val (value, unit) = {
      if (ns >= 1 * HR) {
        (ns.asInstanceOf[Double] / HR, "h")
      } else if (ns >= 1 * MT) {
        (ns.asInstanceOf[Double] / MT, "min")
      } else if (ns >= 1 * SEC) {
        (ns.asInstanceOf[Double] / SEC, "s")
      } else {
        (ns.asInstanceOf[Double] / MS, "ms")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /** Formats a time value in milliseconds using the nanosecond formatter. */
  def formatMilliSeconds(ms: Long): String = formatNanoSeconds(ms * 1000000)

  /** Converts a size in bytes to gigabytes (rounded down). */
  def toGB(bytes: Long): Int = {
    val kilobyte = 1024
    val megabyte = kilobyte * 1024
    val gigabyte = megabyte * 1024
    (bytes / gigabyte).toInt
  }

  /** Converts a size in bytes to megabytes (rounded down). */
  def toMB(bytes: Long): Int = {
    val kilobyte = 1024
    val megabyte = kilobyte * 1024
    (bytes / megabyte).toInt
  }

  /** Converts a size in bytes to gigabytes (as double). */
  def asGB(bytes: Long): Double = {
    val kilobyte = 1024
    val megabyte = kilobyte * 1024
    val gigabyte = megabyte * 1024
    bytes.toDouble / gigabyte
  }

  /**
   * Normalizes a command-line parameter name by removing dashes.
   * Useful for converting `--driver-memory` into `drivermemory`.
   *
   * @param name Original CLI parameter name.
   * @return Normalized identifier-safe name.
   */
  def normalizeName(name: String): String = {
    name.replaceAll("-", "")
  }

}
