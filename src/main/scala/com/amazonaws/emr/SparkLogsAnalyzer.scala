package com.amazonaws.emr

import com.amazonaws.emr.report.HtmlReport
import com.amazonaws.emr.spark.EmrSparkLogParser
import com.amazonaws.emr.utils.Constants._

import scala.annotation.tailrec
import scala.sys.exit

object SparkLogsAnalyzer extends App {

  private val optParams = List(ParamBucket, ParamDuration, ParamExecutors, ParamRegion, ParamSpot)
  private val appParams = (ParamJar :: optParams) :+ ParamLogs
  private val usage =
    s"""
       |  usage: spark-submit --class com.amazonaws.emr.SparkLogsAnalyzer ${appParams.map(_.template).mkString(" ")}
       |
       |  ${appParams.map(_.example).mkString("\n  ")}
       |
       |""".stripMargin

  @tailrec
  private def parseArgs(map: Map[String, String], list: List[String]): Map[String, String] = {
    list match {
      case Nil => map
      case x :: value :: tail if x.startsWith("--") =>
        if (optParams.exists(_.option == x)) parseArgs(map ++ Map(x.replaceAll("-", "") -> value), tail)
        else {
          println(usage)
          println("Error: Unknown option " + x)
          exit(1)
        }
      case string :: Nil =>
        parseArgs(map ++ Map("filename" -> string), list.tail)
      case unknown :: _ =>
        println(usage)
        println("Error: Unknown option " + unknown)
        exit(1)
    }
  }

  if (args.length < 1) println(usage)

  val options = parseArgs(Map(), args.toList)
  val sparkEventLogFile = options.getOrElse("filename", "")

  val logParser = new EmrSparkLogParser(sparkEventLogFile)
  val appContext = logParser.process()
  logParser.analyze(appContext, options)

  HtmlReport.generateReport(appContext, options)

}
