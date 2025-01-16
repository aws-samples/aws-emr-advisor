package com.amazonaws.emr.utils

case class AppParam(name: String, description: String = "", isOptional: Boolean = true) {

  def option: String = s"--$name"

  def template: String =
    if (isOptional) s"[$option ${name.toUpperCase}]"
    else s"<${name.toUpperCase}>"

  def example: String = f"  ${format(name.toUpperCase)} $description"

  private def format(text: String): String = {
    val fixedWidth = 25
    text.padTo(fixedWidth, ' ')
  }

}