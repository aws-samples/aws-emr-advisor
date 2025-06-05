package com.amazonaws.emr.report.spark

import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.models.AppConfigs

/**
 * PageConfigs generates an HTML report page displaying all runtime configurations
 * associated with a Spark application.
 *
 * This includes categorized tables for:
 *   - Hadoop configuration properties
 *   - Java system properties
 *   - Spark-specific configuration settings
 *   - Underlying system environment variables
 *
 * These configurations are sourced from the `AppConfigs` model, which aggregates
 * collected settings from event logs.
 *
 * @param appConfigs Aggregated application-level configurations for rendering.
 */
class PageConfigs(appConfigs: AppConfigs) extends HtmlPage {

  // Prepare table data grouped and sorted alphabetically by key
  private val hadoopConfData = appConfigs.hadoopConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList
  private val javaConfData = appConfigs.javaConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList
  private val sparkConfData = appConfigs.sparkConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList
  private val systemConfData = appConfigs.systemConfigs.toSeq.sortBy(_._1).map(x => List(x._1, x._2)).toList

  /** Unique identifier for the page section (used in nav anchors). */
  override def pageId: String = "configs"

  /** Icon name used for the page tab. */
  override def pageIcon: String = "gear-fill"

  /** Display name of the page tab. */
  override def pageName: String = "Configurations"

  /**
   * Generates the HTML content for the configuration page.
   *
   * The page consists of tabbed sections for each configuration group:
   * Hadoop, Java, Spark, and System.
   *
   * @return HTML string representing the tabbed configuration tables.
   */
  override def content: String = htmlNavTabs("currEnvExample", Seq(
    ("hadoopconf", "Hadoop", htmlTable(Nil, hadoopConfData, CssTableStyle)),
    ("javaconf", "Java", htmlTable(Nil, javaConfData, CssTableStyle)),
    ("sparkconf", "Spark", htmlTable(Nil, sparkConfData, CssTableStyle)),
    ("systemconf", "System", htmlTable(Nil, systemConfData, CssTableStyle))
  ), "hadoopconf", "nav-pills border navbar-light bg-light", "mt-3 text-break")

}