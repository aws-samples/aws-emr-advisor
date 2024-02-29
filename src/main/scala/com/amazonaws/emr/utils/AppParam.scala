package com.amazonaws.emr.utils

case class AppParam(name: String, description: String = "", isOptional: Boolean = true) {

  def option: String = s"--$name"

  def template: String = if (isOptional) s"[$option $upper]" else s"<$upper>"

  def example: String = s"  ${format(upper)} $description"

  private def upper = name.toUpperCase()

  private def format(text: String) = {
    val fixed = 25
    val len = text.length
    text + " " * (fixed - len)
  }

}
