package com.amazonaws.emr.utils

import org.apache.spark.network.util.JavaUtils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.TimeUnit._

object Formatter {

  private val DEFAULT_DATE = new SimpleDateFormat("dd/MM/yyyy")
  private val DEFAULT_TIME = new SimpleDateFormat("HH:mm:ss")

  /**
   * A formatted date as string
   *
   * @param time timestamp in ms
   */
  def printDate(time: Long): String = DEFAULT_DATE.format(new Date(time))

  /**
   * A formatted date time as string
   *
   * @param time timestamp in ms
   */
  def printTime(time: Long): String = DEFAULT_TIME.format(new Date(time))

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  def byteStringAsBytes(str: String): Long = JavaUtils.byteStringAsBytes(str)

  /**
   * Convert bytes to a human readable string (e.g 10TB, 2GB)
   *
   * @param size size in bytes
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

  /**
   * Print time as duration string (e.g 01h 02m 03s)
   *
   * @param millis time expressed in milli-seconds
   */
  def printDuration(millis: Long): String = {
    val hours = if (MILLISECONDS.toHours(millis) > 0) f"${MILLISECONDS.toHours(millis)}%02dh" else ""
    val mins = MILLISECONDS.toMinutes(millis) - HOURS.toMinutes(MILLISECONDS.toHours(millis))
    val minutes = if (mins > 0) f"$mins%02dm" else ""
    val seconds = f"${MILLISECONDS.toSeconds(millis) - MINUTES.toSeconds(MILLISECONDS.toMinutes(millis))}%02ds"
    s"""$hours $minutes $seconds"""
  }

  def printDurationStr(millis: Long): String = {
    val hours = if (MILLISECONDS.toHours(millis) > 0) s"${MILLISECONDS.toHours(millis)} hours" else ""
    val mins = MILLISECONDS.toMinutes(millis) - HOURS.toMinutes(MILLISECONDS.toHours(millis))
    val minutes = if (mins > 0) s"$mins minutes and" else ""
    val seconds = s"${MILLISECONDS.toSeconds(millis) - MINUTES.toSeconds(MILLISECONDS.toMinutes(millis))} seconds"
    s"""$hours $minutes $seconds"""
  }

  def roundUp(d: Double): Int = math.ceil(d).toInt

  def printNumber(number: Number): String = java.text.NumberFormat.getIntegerInstance.format(number)

  def pcm(millis: Long): String = {
    val millisForMinutes = millis % (60 * 60 * 1000)

    "%02dh %02dm".format(
      MILLISECONDS.toHours(millis),
      MILLISECONDS.toMinutes(millisForMinutes))
  }

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

  def formatMilliSeconds(ms: Long): String = formatNanoSeconds(ms * 1000000)

  def toGB(bytes: Long): Int = {
    val kilobyte = 1024
    val megabyte = kilobyte * 1024
    val gigabyte = megabyte * 1024
    (bytes / gigabyte).toInt
  }

  def toMB(bytes: Long): Int = {
    val kilobyte = 1024
    val megabyte = kilobyte * 1024
    (bytes / megabyte).toInt
  }

  def asGB(bytes: Long): Double = {
    val kilobyte = 1024
    val megabyte = kilobyte * 1024
    val gigabyte = megabyte * 1024
    bytes.toDouble / gigabyte
  }

}
