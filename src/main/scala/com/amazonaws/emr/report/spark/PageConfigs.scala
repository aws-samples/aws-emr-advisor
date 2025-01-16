package com.amazonaws.emr.report.spark

import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.models.AppConfigs

class PageConfigs(appConfigs: AppConfigs) extends HtmlPage {

  private val hadoopConfData = appConfigs.hadoopConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList
  private val javaConfData = appConfigs.javaConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList
  private val sparkConfData = appConfigs.sparkConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList
  private val systemConfData = appConfigs.systemConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList

  override def pageId: String = "configs"

  override def pageIcon: String = "gear-fill"

  override def pageName: String = "Configurations"

  override def content: String = htmlNavTabs("currEnvExample", Seq(
    ("hadoopconf", "Hadoop", htmlTable(Nil, hadoopConfData, CssTableStyle)),
    ("javaconf", "Java", htmlTable(Nil, javaConfData, CssTableStyle)),
    ("sparkconf", "Spark", htmlTable(Nil, sparkConfData, CssTableStyle)),
    ("systemconf", "System", htmlTable(Nil, systemConfData, CssTableStyle))
  ), "hadoopconf", "nav-pills border navbar-light bg-light", "mt-3 text-break")

}