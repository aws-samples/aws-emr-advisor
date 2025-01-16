package org.apache.spark.utils

import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.utils.Constants.NotAvailable
import org.apache.spark.deploy.SparkHadoopUtil

import scala.annotation.tailrec

object SparkHelper extends SparkHadoopUtil with HtmlBase {

  case class SparkCommand(
    cmd: String,
    args: List[String],
    base: Map[String, String],
    conf: Map[String, String]
  ) {
    val appMainClass: String = base.getOrElse("class", NotAvailable)

    val appScriptJarPath: String = if (isPython) {
      conf.getOrElse("spark.submit.pyFiles", base.getOrElse("primary-py-file", NotAvailable))
    } else {
      if (!cmd.contains("--arg")) {
        args.find(_.endsWith(".jar")).getOrElse(NotAvailable)
      } else {
        base.getOrElse("jar", NotAvailable)
      }
    }

    val appArguments: List[String] = {
      if (!cmd.contains("--arg") && args.nonEmpty) {
        val afterJarArgs = args.dropWhile(_ != appScriptJarPath)
        if (afterJarArgs.nonEmpty) afterJarArgs.tail else args
      } else {
        args
      }
    }

    def submitEc2Step: String = {
      val mainClass = htmlTextRed(appMainClass)
      val jarPath = htmlTextRed(appScriptJarPath)
      val appParams = if (appArguments.nonEmpty) {
        htmlTextRed(appArguments.mkString("\"", "\",\"", "\""))
      } else ""

      if (isScala) {
        s""""spark-submit",
           |        "--class",
           |        "$mainClass",
           |        "$jarPath",
           |        $appParams""".stripMargin
      } else {
        s""""spark-submit",
           |        "$jarPath",
           |        $appParams""".stripMargin
      }
    }

    def isPython: Boolean = cmd.contains(".py")

    def isScala: Boolean = !isPython

    override def toString: String =
      s"""
         |Application Main Class: $appMainClass
         |Arguments: ${args.mkString(" ")}
         |Base: ${base.mkString(", ")}
         |Configuration: ${conf.mkString(", ")}
         |""".stripMargin
  }

  def parseSparkCmd(cmd: String): SparkCommand = {
    val args = cmd.split("\\s+").toList
    val (parsedArgs, base, conf) = parseSparkCmd(args)
    SparkCommand(cmd, parsedArgs, base, conf)
  }

  @tailrec
  private def parseSparkCmd(
    list: List[String],
    base: Map[String, String] = Map.empty,
    args: List[String] = Nil,
    conf: Map[String, String] = Map.empty
  ): (List[String], Map[String, String], Map[String, String]) = {
    list match {
      case Nil =>
        (args.reverse, base, conf)

      case "org.apache.spark.deploy.yarn.ApplicationMaster" :: tail =>
        parseSparkCmd(tail, base, args, conf)

      case "org.apache.spark.deploy.SparkSubmit" :: tail =>
        parseSparkCmd(tail, base, args, conf)

      case "--arg" :: value :: tail =>
        parseSparkCmd(tail, base, value :: args, conf)

      case "--conf" :: value :: tail =>
        val Array(key, v) = value.split("=", 2)
        parseSparkCmd(tail, base, args, conf + (key -> v))

      case key :: value :: tail if key.startsWith("--") =>
        parseSparkCmd(tail, base + (key.stripPrefix("--") -> value), args, conf)

      case param :: tail =>
        parseSparkCmd(tail, base, param :: args, conf)
    }
  }

}
