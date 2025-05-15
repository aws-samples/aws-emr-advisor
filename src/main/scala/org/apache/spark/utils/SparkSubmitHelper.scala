package org.apache.spark.utils

import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.utils.Constants.NotAvailable

import scala.annotation.tailrec

object SparkSubmitHelper extends HtmlBase {

  case class SparkSubmitCommand(
    original: String,
    deployMode: Option[String],
    mainClass: Option[String],
    mainFile: Option[String],
    propertiesFile: Option[String],
    conf: Map[String, String],
    extraArgs: Map[String, String],
    scriptArgs: List[String],
    language: Option[String]
  ) {

    val appMainClass: String = mainClass.getOrElse(NotAvailable)
    val appScriptJarPath: String = mainFile.getOrElse(NotAvailable)
    val appArguments: List[String] = scriptArgs

    def isPython: Boolean = language.contains("python")

    def isScala: Boolean = language.contains("scala") || language.contains("java")

    override def toString: String =
      s"""
         |Language        : ${language.getOrElse(NotAvailable)}
         |Deploy Mode     : ${deployMode.getOrElse(NotAvailable)}
         |Main Class      : ${mainClass.getOrElse(NotAvailable)}
         |Main Script/Jar : ${mainFile.getOrElse(NotAvailable)}
         |Main Arguments  : ${scriptArgs.mkString(" ")}
         |Properties      : ${propertiesFile.getOrElse(NotAvailable)}
         |Configuration   : ${conf.mkString(", ")}
         |Extra Args      : ${extraArgs.mkString(", ")}
         |""".stripMargin
  }

  def parse(cmd: String): SparkSubmitCommand = {
    val tokens = cmd.split("\\s+").toList
    val (_, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs) = parseTokens(tokens)
    val language = detectLanguage(mainFile)
    SparkSubmitCommand(cmd, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs, language)
  }

  private def detectLanguage(mainFile: Option[String]): Option[String] = {
    mainFile match {
      case Some(file) if file.endsWith(".py") => Some("python")
      case Some(file) if file.endsWith(".jar") => Some("scala")
      case _ => None
    }
  }

  @tailrec
  private def parseTokens(
    tokens: List[String],
    deployMode: Option[String] = None,
    mainClass: Option[String] = None,
    mainFile: Option[String] = None,
    propsFile: Option[String] = None,
    conf: Map[String, String] = Map.empty,
    extraArgs: Map[String, String] = Map.empty,
    scriptArgs: List[String] = Nil,
    seenMainFile: Boolean = false
  ): (List[String], Option[String], Option[String], Option[String], Option[String], Map[String, String], Map[String, String], List[String]) = {

    tokens match {
      case Nil =>
        (tokens, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs)

      case "org.apache.spark.deploy.SparkSubmit" :: tail =>
        parseTokens(tail, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs, seenMainFile)

      case "org.apache.spark.deploy.yarn.ApplicationMaster" :: tail =>
        parseTokens(tail, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs, seenMainFile)

      case "--deploy-mode" :: mode :: tail =>
        parseTokens(tail, Some(mode), mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs, seenMainFile)

      case "--class" :: clazz :: tail =>
        parseTokens(tail, deployMode, Some(clazz), mainFile, propsFile, conf, extraArgs, scriptArgs, seenMainFile)

      case "--primary-py-file" :: py :: tail =>
        parseTokens(tail, deployMode, mainClass, Some(py), propsFile, conf, extraArgs, scriptArgs, seenMainFile = true)

      case "--jar" :: jar :: tail =>
        parseTokens(tail, deployMode, mainClass, Some(jar), propsFile, conf, extraArgs, scriptArgs, seenMainFile = true)

      case "--properties-file" :: path :: tail =>
        parseTokens(tail, deployMode, mainClass, mainFile, Some(path), conf, extraArgs, scriptArgs, seenMainFile)

      case "--conf" :: keyVal :: tail =>
        val Array(key, value) = keyVal.split("=", 2)
        parseTokens(tail, deployMode, mainClass, mainFile, propsFile, conf + (key -> value), extraArgs, scriptArgs, seenMainFile)

      case "--arg" :: value :: tail =>
        parseTokens(tail, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs :+ value, seenMainFile)

      case arg :: value :: tail if arg.startsWith("--") =>
        parseTokens(tail, deployMode, mainClass, mainFile, propsFile, conf, extraArgs + (arg -> value), scriptArgs, seenMainFile)

      case file :: tail if (file.endsWith(".py") || file.endsWith(".jar")) && !seenMainFile =>
        parseTokens(tail, deployMode, mainClass, Some(file), propsFile, conf, extraArgs, scriptArgs, seenMainFile = true)

      case arg :: tail if seenMainFile =>
        parseTokens(tail, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs :+ arg, seenMainFile)

    }
  }

}
