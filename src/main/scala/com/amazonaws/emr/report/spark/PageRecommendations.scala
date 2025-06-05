package com.amazonaws.emr.report.spark

import com.amazonaws.emr.Config
import com.amazonaws.emr.api.AwsPricing.DefaultCurrency
import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.analyzer.AppRuntimeEstimate
import com.amazonaws.emr.spark.models.OptimalTypes._
import com.amazonaws.emr.spark.models.runtime.Environment.{emptyEmrOnEc2, emptyEmrOnEks, emptyEmrServerless}
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, Environment, SparkRuntime}
import com.amazonaws.emr.spark.models.{AppInfo, AppRecommendations}
import com.amazonaws.emr.utils.Formatter._

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration

class PageRecommendations(
  id: String,
  name: String,
  icon: String,
  optType: OptimalType,
  appInfo: AppInfo,
  appRecommendations: AppRecommendations
) extends HtmlPage {

  override def pageId: String = id

  override def pageIcon: String = icon

  override def pageName: String = name

  override def subSection: String = "Recommendations"

  override def content: String = {

    val ec2 = appRecommendations.getRecommendations(Environment.EC2).getOrElse(optType, emptyEmrOnEc2)
    val eks = appRecommendations.getRecommendations(Environment.EKS).getOrElse(optType, emptyEmrOnEks)
    val svl = appRecommendations.getRecommendations(Environment.SERVERLESS).getOrElse(optType, emptyEmrServerless)

    val summaryHtml = summaryTab(ec2, eks, svl)
    val htmlTabPage = htmlNavTabs(id = optType.toString + "recommendedTab", tabs = Seq(
      ("recSummary" + optType.toString, "Summary", summaryHtml),
      ("recEc2" + optType.toString, ec2.label, environmentTab(ec2)),
      ("recEks" + optType.toString, eks.label, environmentTab(eks)),
      ("recSvl" + optType.toString, svl.label, environmentTab(svl)),
    ), "recSummary" + optType.toString, "nav-pills border navbar-light bg-light")
    htmlTabPage

  }

  /* Create a detailed tab dedicated to a specific deployment environment. */
  private def environmentTab(environment: EmrEnvironment): String =
    s"""
       |<div class="row mt-3">
       |  <div class="col">
       |    ${environment.htmlCard(extended = true)}
       |  </div>
       |  <div class="col-8">
       |    <div class="card">
       |      <div class="card-header">
       |        <div class="float-start m-2 mt-2 mb-0" style="padding-right:25px!important">
       |          <i class="bi bi-graph-up-arrow h1" style="color:gray;"></i>
       |        </div>
       |        <h5 class="card-title pt-2">Simulations</h5>
       |        <h6 class="card-subtitle text-muted float-start">Application execution times for varying numbers of executors</h6>
       |      </div>
       |      <div class="card-body position-relative">
       |        ${simulations(environment)}
       |      </div>
       |    </div>
       |    <div class="card mt-3">
       |      <div class="card-header">
       |        <h5 class="card-title pt-2">Apache Spark</h5>
       |        <h6 class="card-subtitle text-muted float-start">Recommended Apache Spark configurations</h6>
       |      </div>
       |      <div class="card-body position-relative">
       |        ${spark(appRecommendations.currentSparkConf.get, environment.sparkRuntime)}
       |      </div>
       |    </div>
       |  </div>
       |</div>
       |
       |<div class="row mt-3">
       |  <div class="col">
       |    <div class="card">
       |      <div class="card-header">
       |        <div class="float-start m-2 mt-2 mb-0" style="padding-right:25px!important">
       |          <i class="bi bi-terminal h1" style="color:gray;"></i>
       |        </div>
       |        <h5 class="card-title pt-2">Examples</h5>
       |        <h6 class="card-subtitle text-muted float-start">Sample commands to validate the configurations</h6>
       |      </div>
       |      <div class="card-body position-relative">
       |        ${environment.htmlExample(appInfo)}
       |      </div>
       |    </div>
       |  </div>
       |</div>
       |""".stripMargin

  /* Create a comparative summary tab for all deployment models. */
  private def summaryTab(ec2: EmrEnvironment, eks: EmrEnvironment, svl: EmrEnvironment): String = {

    val awsRegion = ec2.costs.region
    val environments = List(
      (ec2.label, ec2.costs.total, ec2),
      (eks.label, eks.costs.total, eks),
      (svl.label, svl.costs.total, svl)
    )

    val cheaper = environments.minBy(_._2)
    val sparkRuntime = cheaper._3.sparkRuntime
    val (ec2Selected, eksSelected, svlSelected) = cheaper._3.label match {
      case x if x == ec2.label => (true, false, false)
      case x if x == eks.label => (false, true, false)
      case x if x == svl.label => (false, false, true)
      case _ => (false, false, false)
    }

    val suggested =
      s"""Based on the recommended configurations and projected runtime, your application is best suited to run on
         | ${htmlBold(cheaper._1)}. The estimated runtime is ${printDurationStr(sparkRuntime.runtime)}, with a total cost of
         | <b>${"%.2f".format(cheaper._2)} $DefaultCurrency</b> in the ${htmlBold(awsRegion)} region.""".stripMargin

    val userInfo =
      "For more information and examples, explore the respective Environment tab for each deployment option."
    val recommendationInfo = optType match {
      case CostOpt =>
        s"""|${htmlBold("Cost-optimized recommendations")} focus on identifying the ${htmlBold("cheapest environment")}
            |where the original Spark job configuration can successfully run. The Spark settings used in this analysis
            |are based on the ${htmlBold("default configuration detected from the original job")}, including driver and
            |executor resources. These configurations are not altered unless they are incompatible with the target
            |environment—for example, if they violate EMR Serverless worker constraints. The goal is to maintain the
            |application's original resource profile as closely as possible while minimizing total cost, including
            |compute, memory, and storage across EC2, EKS, or Serverless options.
            |""".stripMargin
      case EfficiencyOpt =>
        s"""|${htmlBold("Efficiency-optimized recommendations")} aim to identify the
            |${htmlBold("most resource-efficient Spark configuration")} based on actual job behavior and metrics.
            |Unlike cost-optimized recommendations, the original Spark configurations are
            |${htmlBold("adjusted and optimized")} to request only the minimal CPU, memory, and storage needed to run
            |the job effectively, as inferred from Spark runtime metrics such as peak memory usage, spill volume, and
            |result size. Once an optimal resource configuration is determined, the system evaluates all supported
            |environments (EC2, EKS, and Serverless) and selects the one that can
            |${htmlBold("run the job at the lowest total cost")} using that efficient setup.""".stripMargin
      case PerformanceOpt =>
        s"""|${htmlBold("Performance-optimized recommendations")} focus on tuning Spark resource configurations to
           |${htmlBold("maximize runtime efficiency and speed")}, even if it means provisioning slightly more resources
           |than strictly required. As with efficiency-optimized recommendations, the original Spark settings are
           |${htmlBold("dynamically adjusted")} based on observed application metrics to determine the optimal CPU,
           |memory, and storage needed.""".stripMargin
      case UserDefinedOpt =>
        s"""|${htmlBold("User-defined recommendations")} optimize Spark configurations to meet a
            |${htmlBold("target runtime")} while minimizing cost. Simulations that complete within the
            |specified duration are ${htmlBold("filtered and scored")} based on total cost.
            |Spark settings are ${htmlBold("dynamically adjusted")} using application metrics, and the
            |most cost-effective option is selected across supported environments (EC2, EKS, Serverless).
            |""".stripMargin
    }

    s"""
       |<div class="row row-cols-1 row-cols-md-3 mt-3">
       |  <div class="col">${ec2.htmlCard(selected = ec2Selected)}</div>
       |  <div class="col">${eks.htmlCard(selected = eksSelected)}</div>
       |  <div class="col">${svl.htmlCard(selected = svlSelected)}</div>
       |</div>
       |
       |<p class="mt-3">${htmlBoxSuccess(suggested)}</p>
       |<p class="mt-3">${htmlBoxInfo(recommendationInfo)}</p>
       |<p class="mt-3">${htmlBoxInfo(userInfo)}</p>
       |""".stripMargin
  }

  /* Generate a simulation graph based on the data used to derive the recommended Spark settings. */
  private def simulations(environment: EmrEnvironment): String = {

    val randomStringId: String = UUID.randomUUID().toString
    val simulations:Seq[(Int, AppRuntimeEstimate)] = environment.simulations
      .getOrElse(Seq.empty)
      .filter(_.coresPerExecutor == environment.sparkRuntime.executorCores)
      .map(s => s.executorNum -> s.appRuntimeEstimate)
      .toMap
      .toSeq

    val sortedSimulations = SortedMap(simulations: _*)

    val recommended = environment.sparkRuntime.executorsNum
    val visibleTests = Config.ExecutorsMaxVisibleTestsCount
    val data = takeCenteredElements(sortedSimulations, recommended, visibleTests)

    val labels = data.keys.toList
    val values = data.values.map(a => Duration(a.estimatedAppTimeMs, TimeUnit.MILLISECONDS).toSeconds.toInt).toList
    val explain = htmlGroupList(
      List(
        s"""
           |The graph depicts the expected application runtime for varying Spark executor counts, ranging from
           |${htmlBold("1")} to ${htmlBold(simulations.size.toString)}. Each simulation assumes uniform executors
           |with ${htmlBold(s"${environment.sparkRuntime.executorCores}")} cores each.
           |""".stripMargin,
        s"""The recommended number of executors for this job is <b>$recommended</b>. Using this value, the expected
           |runtime for the job is <b>${printDurationStr(environment.sparkRuntime.runtime)}</b>
           |""".stripMargin
      ), "list-group-flush")

    s"""
       |${htmlSimulationGraph(s"sim-$randomStringId", recommended, labels, values, 25)}
       |$explain
       |""".stripMargin

  }

  /* Create a comparison table to review previous Spark settings with the recommended ones. */
  private def spark(current: SparkRuntime, optimal: SparkRuntime): String = {

    val sparkTable = htmlTable(
      List("", "Original", "Recommended"),
      List(
        List("Application Runtime", printDurationStr(current.runtime), printDurationStr(optimal.runtime) + htmlTextSmall(" (estimated) *")),
        List("Driver Cores", s"${current.driverCores}", s"${optimal.driverCores}"),
        List("Driver Memory", humanReadableBytes(current.driverMemory), humanReadableBytes(optimal.driverMemory)),
        List("Executor Cores", s"${current.executorCores}", s"${optimal.executorCores}"),
        List("Executor Memory", humanReadableBytes(current.executorMemory), humanReadableBytes(optimal.executorMemory)),
        List("Max Executors", s"${current.executorsNum}", s"${optimal.executorsNum}")
      ), s"""$CssTableStyle mb-0""")

    val explain = htmlGroupList(
      List(
        htmlTextSmall(s"""
           |* The estimated Spark runtime is based on your job's metrics. Failures at the job, stage, or task level may
           |affect its accuracy. Please treat this estimate as a reference, not an absolute value, as it may lead to
           |incorrect expectations.
           |""".stripMargin)
      ), "list-group-flush")

    s"""
       |<div class="app-recommendations spark">
       |  $sparkTable
       |  $explain
       |</div>
       |""".stripMargin

  }

  private def takeCenteredElements(
    map: SortedMap[Int, AppRuntimeEstimate],
    centerKey: Int,
    n: Int
  ): SortedMap[Int, AppRuntimeEstimate] = {
    val elements = n / 2
    val totalKeys = map.keys.toSeq
    val centerIndex = totalKeys.indexOf(centerKey)

    if (centerIndex == -1) map
    else {
      val startIndex = math.max(0, centerIndex - elements)
      val endIndex = math.min(totalKeys.size, centerIndex + elements + 1)
      val keysInRange = totalKeys.slice(startIndex, endIndex)
      map.filterKeys(keysInRange.contains)
    }
  }

}
