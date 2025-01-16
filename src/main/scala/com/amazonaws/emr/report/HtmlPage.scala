package com.amazonaws.emr.report

trait HtmlPage extends HtmlBase {

  def content: String

  def pageId: String

  def pageName: String

  def pageIcon: String = ""

  def isActive: Boolean = false

  def subSection: String = ""

}
