package org.apache.spark.utils

import com.amazonaws.emr.report.HtmlReport.htmlTextRed
import com.amazonaws.emr.utils.Constants.NotAvailable
import org.apache.spark.deploy.SparkHadoopUtil

import scala.annotation.tailrec

object SparkHelper extends SparkHadoopUtil {

  case class SparkCommand(cmd: String, args: List[String], base: Map[String, String], conf: Map[String, String]) {

    val appMainClass: String = base.getOrElse("class", NotAvailable)
    val appScriptJarPath: String = if (isPython) {

      if (conf.contains("spark.submit.pyFiles")) {
        conf.getOrElse("spark.submit.pyFiles", NotAvailable)
      } else if (base.contains("primary-py-file")) {
        base.getOrElse("primary-py-file", NotAvailable)
      } else NotAvailable

    } else {
      if (!cmd.contains("--arg")) {
        args.head
      } else {
        base.getOrElse("jar", NotAvailable)
      }
    }

    val appArguments: List[String] = {
      if (!cmd.contains("--arg") && args.nonEmpty) {
        args.tail
      } else {
        args
      }
    }

    def submitEc2Step: String = {
      val mainClass = htmlTextRed(appMainClass)
      val jarPath = htmlTextRed(appScriptJarPath)
      val appParams = if(appArguments.nonEmpty) {
        htmlTextRed(appArguments.mkString("\"", "\",\"", "\""))
      } else ""

      if(isScala) s""""spark-submit","--class","$mainClass","$jarPath",$appParams"""
      else s""""spark-submit","$jarPath"$appParams"""
    }

    def isPython: Boolean = if (cmd.nonEmpty && cmd.contains(".py")) true else false

    def isScala: Boolean = !isPython

    override def toString: String =
      s"""
         |$appMainClass
         |args: ${args.mkString(" ")}
         |base: ${base.mkString(" ")}
         |conf: ${conf.mkString(" ")}
         |""".stripMargin
  }

  def parseSparkCmd(cmd: String): SparkCommand = {
    val args = cmd.split("\\s+").toList
    val parsed = parseSparkCmd(args, Map(), Nil, Map())
    SparkCommand(cmd, parsed._1, parsed._2, parsed._3)
  }

  @tailrec
  private final def parseSparkCmd(
    list: List[String],
    base: Map[String, String],
    args: List[String] = Nil,
    conf: Map[String, String] = Map()
  ): (List[String], Map[String, String], Map[String, String]) = {
    list match {
      case Nil => (args.reverse, base, conf)
      case "org.apache.spark.deploy.yarn.ApplicationMaster" :: tail =>
        parseSparkCmd(tail, base, args, conf)
      case "org.apache.spark.deploy.SparkSubmit" :: tail =>
        parseSparkCmd(tail, base, args, conf)
      case "--arg" :: value :: tail =>
        parseSparkCmd(tail, base, value :: args, conf)
      case "--conf" :: value :: tail =>
        val k = value.split('=')(0)
        val v = value.split('=')(1)
        parseSparkCmd(tail, base, args, conf ++ Map(k -> v))
      case key :: value :: tail if key.startsWith("--") =>
        parseSparkCmd(tail, base ++ Map(key.drop(2) -> value), args, conf)
      case param :: tail if param.nonEmpty =>
        parseSparkCmd(tail, base, param :: args, conf)
    }
  }

}
