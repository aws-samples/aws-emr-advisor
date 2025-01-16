package com.amazonaws.emr.report

import com.amazonaws.emr.report.spark._
import com.amazonaws.emr.spark.models.AppContext
import com.amazonaws.emr.spark.models.OptimalTypes._
import org.apache.logging.log4j.scala.Logging

class ReportSparkSingleJob(
  appContext: AppContext,
  hasCustomSettings: Boolean = false
) extends HtmlReport with Logging {

  override def title: String = appContext.appInfo.applicationID

  override def reportName: String = "Spark"

  override def reportPrefix: String = "spark"

  override val CssCustom: String =
    s"""
       |  .app-summary tbody tr td:first-child {
       |    width: 15em;
       |    min-width: 15em;
       |    max-width: 15em;
       |    word-break: break-all;
       |    font-weight: bold;
       |    color: gray;
       |  }
       |
       |  .app-configs tbody tr td:first-child {
       |    width: 35em;
       |    min-width: 35em;
       |    max-width: 35em;
       |    word-break: break-all;
       |    font-weight: normal;
       |  }
       |
       |  .app-metrics tbody tr td {
       |    width: 20%;
       |    min-width: 20%;
       |    max-width: 20%;
       |    word-break: break-all;
       |    font-weight: normal;
       |  }
       |
       |  .app-metrics .jobs tbody tr td {
       |    width: 16%;
       |    min-width: 16%;
       |    max-width: 16%;
       |    word-break: break-all;
       |    font-weight: normal;
       |  }
       |
       |  .app-recommendations.spark tbody tr td:first-child {
       |    width: 12em;
       |    min-width: 12em;
       |    max-width: 12em;
       |    word-break: break-all;
       |    font-weight: normal;
       |  }
       |
       |  .card tbody tr td:first-child {
       |    width: auto;
       |    min-width: auto;
       |    word-break: break-all;
       |    font-weight: normal;
       |  }
       |
       |  #executorTable tbody tr td {
       |    width: 5%;
       |    min-width: 5%;
       |    max-width: 10%;
       |    word-break: break-all;
       |    font-weight: normal;
       |  }
       |
       |""".stripMargin

  override def pages: List[HtmlPage] = {
    val basePages = List(
      new PageSummary(appContext.appInfo, appContext.appConfigs),
      new PageConfigs(appContext.appConfigs),
      new PageMetrics(
        appContext.appMetrics,
        appContext.jobMap,
        appContext.appSparkExecutors,
        appContext.appEfficiency
      ),
      new PageRecommendations(
        "costoptrecommendations", "Costs", "currency-dollar",
        CostOpt, appContext.appInfo, appContext.appRecommendations
      ),
      new PageRecommendations(
        "timeoptrecommendations", "Performance", "clock",
        TimeOpt, appContext.appInfo, appContext.appRecommendations
      ),
    )

    if (hasCustomSettings) basePages :+ new PageRecommendations(
      "userdefinedrecommendations", "User Defined", "person-circle",
      UserDefinedOpt, appContext.appInfo, appContext.appRecommendations)
    else basePages

  }

}
