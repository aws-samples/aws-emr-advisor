package com.amazonaws.emr.utils

import com.amazonaws.emr.utils.Formatter.normalizeName

import scala.annotation.tailrec
import scala.sys.exit

class ArgParser(
  cmd: String,
  required: Int,
  appParams: List[AppParam],
  optParams: List[AppParam]
) {

  private val appHelper: String =
    s"""
       |Usage: $cmd ${appParams.map(_.template).mkString(" ")}
       |
       |Parameters:
       |${appParams.map(_.example).mkString("\n")}
       |""".stripMargin

  /**
   * Processes input arguments passed to the application.
   *
   * @param args List of application parameters.
   * @return Map of parsed parameters.
   */
  def parse(args: List[String]): Map[String, String] = {
    if (args.length < required) {
      printlnError("Insufficient arguments provided.")
    }
    parseArgs(Map.empty, args)
  }

  @tailrec
  private def parseArgs(parsed: Map[String, String], remaining: List[String]): Map[String, String] = {
    remaining match {
      case Nil => parsed

      case option :: value :: tail if option.startsWith("--") =>
        if (optParams.exists(_.option == option)) {
          parseArgs(parsed + (normalizeName(option) -> value), tail)
        } else {
          printlnError(s"Unknown option: $option")
        }

      case singleValue :: Nil =>
        parseArgs(parsed + ("filename" -> singleValue), Nil)

      case unknown :: _ =>
        printlnError(s"Unknown argument: $unknown")
    }
  }

  private def printlnError(message: String): Nothing = {
    println(appHelper)
    println(s"Error: $message")
    exit(1)
  }
}