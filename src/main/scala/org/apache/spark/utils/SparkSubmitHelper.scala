package org.apache.spark.utils

import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.utils.Constants.NotAvailable

import scala.annotation.tailrec

/**
 * SparkSubmitHelper provides utilities to parse raw Spark submit commands
 * into structured representations for analysis and rendering purposes.
 *
 * This object is used to deconstruct a Spark command string (typically
 * from logs or execution metadata) into a `SparkSubmitCommand`, extracting
 * key components such as deploy mode, main class or script, configuration
 * properties, and arguments.
 */
object SparkSubmitHelper extends HtmlBase {


  /**
   * Case class representing a parsed Spark submit command.
   *
   * @param original       The original raw command string.
   * @param deployMode     Optional Spark deploy mode ("cluster" or "client").
   * @param mainClass      Main class to run (for Scala/Java apps).
   * @param mainFile       Path to the primary script or JAR.
   * @param propertiesFile Optional path to a properties file.
   * @param conf           Key-value pairs from `--conf` arguments.
   * @param extraArgs      Other named arguments in the format `--arg value`.
   * @param scriptArgs     Positional arguments passed to the application.
   * @param language       Detected language ("python" or "scala").
   */
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

    /** Extracted main class, or NotAvailable if not provided. */
    val appMainClass: String = mainClass.getOrElse(NotAvailable)

    /** Path to the main JAR or Python script, or NotAvailable if missing. */
    val appScriptJarPath: String = mainFile.getOrElse(NotAvailable)

    /** Arguments passed to the application. */
    val appArguments: List[String] = scriptArgs

    /** Checks if the application is a Python Spark app. */
    def isPython: Boolean = language.contains("python")

    /** Checks if the application is a Scala or Java Spark app. */
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

  /**
   * Parses a raw Spark submit command string into a structured SparkSubmitCommand.
   *
   * @param cmd Raw spark-submit command string.
   * @return Parsed SparkSubmitCommand instance.
   */
  def parse(cmd: String): SparkSubmitCommand = {
    val tokens = cmd.split("\\s+").toList
    val (_, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs) = parseTokens(tokens)
    val language = detectLanguage(mainFile)
    SparkSubmitCommand(cmd, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs, language)
  }

  /**
   * Infers the application language based on the main file extension.
   *
   * @param mainFile Path to the main file.
   * @return Some("python") or Some("scala") if file extension matches, None otherwise.
   */
  private def detectLanguage(mainFile: Option[String]): Option[String] = {
    mainFile match {
      case Some(file) if file.endsWith(".py") => Some("python")
      case Some(file) if file.endsWith(".jar") => Some("scala")
      case _ => None
    }
  }

  /**
   * Recursively parses command-line tokens to extract Spark submit options.
   *
   * Recognizes keys such as --deploy-mode, --class, --conf, etc., and
   * separates them from application arguments.
   *
   * @param tokens        Remaining tokens to process.
   * @param deployMode    Optional deploy mode.
   * @param mainClass     Optional main class.
   * @param mainFile      Optional main script or JAR path.
   * @param propsFile     Optional properties file path.
   * @param conf          Accumulated Spark configuration options.
   * @param extraArgs     Extra key-value arguments.
   * @param scriptArgs    Application-specific arguments.
   * @param seenMainFile  Whether a main file (JAR or script) has been encountered.
   * @return Tuple with updated values after parsing.
   */
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

      case _ =>
        (tokens, deployMode, mainClass, mainFile, propsFile, conf, extraArgs, scriptArgs)
    }
  }

}
