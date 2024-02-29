package com.amazonaws.emr.report

trait HtmlPage {

  val CssTableStyle = "table-bordered table-striped table-sm"

  def render: String

}
