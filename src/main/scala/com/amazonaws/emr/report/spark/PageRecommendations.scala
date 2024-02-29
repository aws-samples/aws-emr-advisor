package com.amazonaws.emr.report.spark

import com.amazonaws.emr.Config.EbsDefaultStorage
import com.amazonaws.emr.utils.Constants._
import com.amazonaws.emr.api.AwsPricing.DefaultCurrency
import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.models.runtime.Environment.{emptyEmrOnEc2, emptyEmrOnEks, emptyEmrServerless}
import com.amazonaws.emr.spark.models.{AppInfo, AppRecommendations}
import com.amazonaws.emr.spark.models.runtime.{EmrEnvironment, EmrOnEksEnv, SparkRuntime}
import com.amazonaws.emr.report.HtmlReport._
import com.amazonaws.emr.utils.Formatter._

class PageRecommendations(appInfo: AppInfo, appRecommendations: AppRecommendations) extends HtmlPage {

  private def render(
    ec2: EmrEnvironment,
    eks: EmrEnvironment,
    svl: EmrEnvironment,
    sparkCurr: SparkRuntime,
    sparkOpt: SparkRuntime
  ): String = {

    val htmlTabPage = htmlNavTabs(id = "recommendedTab", tabs = Seq(
      ("recDeployment", "Deployments", deploymentsTab(ec2, eks, svl)),
      ("recExamples", "Commands / Examples", exampleTab(ec2, eks, svl, sparkOpt)),
      ("recSpark", "Spark Settings", sparkTab(sparkCurr, sparkOpt))
    ), "recDeployment", "nav-pills border navbar-light bg-light")
    htmlTabPage
  }

  def deploymentsTab(ec2: EmrEnvironment, eks: EmrEnvironment, svl: EmrEnvironment): String = {
    val awsRegion = ec2.costs.region
    val environments = List((ec2.label, ec2.costs.total), (eks.label, eks.costs.total), (svl.label, svl.costs.total))
    val cheaper = environments.minBy(_._2)

    val suggested = htmlBoxNote(
      s"""Based on suggested configurations and projected runtime, your application will benefit running within
         |${htmlBold(cheaper._1)}, with an estimated total costs of <b>${"%.2f".format(cheaper._2)} $DefaultCurrency</b>
         | in the ${htmlBold(awsRegion)} region""".stripMargin
    )

    s"""<!-- Emr Deployment Recommended -->
       |<div class="row mt-4">
       |  <div class="col-sm">
       |    $suggested
       |  </div>
       |</div>
       |
       |<!-- Emr Deployments -->
       |<div class="card-group">
       |  <div class="card">
       |    <div class="card-header">
       |      <div class="float-start p-2">
       |        $HtmlSvgEmrOnEc2
       |      </div>
       |      <h5 class="card-title pt-1">EMR on EC2</h5>
       |      <h6 class="card-subtitle text-muted float-start">Managed Hadoop running on Amazon EC2</h6>
       |    </div>
       |    <div class="card-body position-relative">
       |      ${ec2.toHtml}
       |    </div>
       |    <div class="card-footer text-muted">
       |      <div class="float-start"><b>Total Costs</b></div>
       |      <div class="float-end">${"%.2f".format(ec2.costs.total)} $DefaultCurrency</div>
       |    </div>
       |  </div>
       |
       |  <div class="card">
       |    <div class="card-header">
       |      <div class="float-start p-2">
       |        $HtmlSvgEmrOnEks
       |      </div>
       |      <h5 class="card-title pt-1">EMR on EKS</h5>
       |      <h6 class="card-subtitle text-muted float-start">Run your Spark workloads on Amazon EKS</h6>
       |    </div>
       |    <div class="card-body position-relative">
       |      ${eks.toHtml}
       |    </div>
       |    <div class="card-footer text-muted">
       |      <div class="float-start"><b>Total Costs</b></div>
       |      <div class="float-end">${"%.2f".format(eks.costs.total)} $DefaultCurrency</div>
       |    </div>
       |  </div>
       |
       |  <div class="card">
       |    <div class="card-header">
       |      <div class="float-start p-2">
       |        $HtmlSvgEmrServerless
       |      </div>
       |      <h5 class="card-title pt-1">EMR Serverless</h5>
       |      <h6 class="card-subtitle text-muted float-start">Run your Spark workloads on EMR Serverless</h6>
       |    </div>
       |    <div class="card-body position-relative">
       |      ${svl.toHtml}
       |    </div>
       |    <div class="card-footer text-muted">
       |      <div class="float-start"><b>Total Costs</b></div>
       |      <div class="float-end">${"%.2f".format(svl.costs.total)} $DefaultCurrency</div>
       |    </div>
       |  </div>
       |</div>
       |""".stripMargin
  }

  def exampleTab(ec2: EmrEnvironment, eks: EmrEnvironment, svl: EmrEnvironment, sparkRuntime: SparkRuntime): String = {
    val examplesNavTabs = htmlNavTabs("currEnvExample", Seq(
      ("emrOnEc2Cmd", ec2.label,
        s"""1. (Optional) Create default ${htmlLink("IAM Roles for EMR", LinkEmrOnEc2IamRoles)}
           |${htmlCodeBlock(ec2.exampleCreateIamRoles, "bash")}
           |2. Review the parameters and launch an EC2 cluster to test the configurations
           |${htmlCodeBlock(ec2.exampleSubmitJob(appInfo, sparkRuntime), "bash")}
           |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR", LinkEmrOnEc2QuickStart)}
           |in the AWS Documentation.</p>""".stripMargin),
      ("emrOnEksCmd", eks.label,
        s"""1. (Optional) Create an ${htmlLink("Amazon EKS cluster with Karpenter", LinkEmrOnEksKarpenterGettingStarted)}
           | and setup an ${htmlLink("EMR on EKS", LinkEmrOnEksQuickStart)} cluster with the ${htmlLink("Spark Operator", LinkEmrOnEksSparkOperator)}
           |<br/><br/>
           |2. Create a Storage class to mount dynamically-created ${htmlLink("persistent volume claim", LinkSparkK8sPvc)} on the Spark executors using ${EbsDefaultStorage.toUpperCase} EBS volumes
           |${htmlCodeBlock(eks.asInstanceOf[EmrOnEksEnv].exampleCreateStorageClass, "bash")}
           |3. Create a custom provisioner to scale the cluster
           |${htmlCodeBlock(eks.exampleRequirements(appInfo), "bash")}
           |4. Review the parameters and submit the application using the Spark Operator
           |${htmlCodeBlock(eks.exampleSubmitJob(appInfo, sparkRuntime), "bash")}
           |<p>For additional details, see ${htmlLink("Running jobs with Amazon EMR on EKS", LinkEmrOnEksJobRunsDoc)}
           |in the EMR Documentation.</p>""".stripMargin),
      ("emrServerlessCmd", svl.label,
        s"""1. (Optional) ${htmlLink("Create an IAM Job Role", LinkEmrServerlessJobRoleDoc)}
           |<br/><br/>
           |2. Create an Emr Serverless Application using the latest EMR release to submit your Spark job
           |${htmlCodeBlock(svl.exampleRequirements(appInfo), "bash")}
           |3. Review the parameters and submit the application
           |${htmlCodeBlock(svl.exampleSubmitJob(appInfo, sparkRuntime), "bash")}
           |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR Serverless", LinkEmrServerlessQuickStart)}
           |in the AWS Documentation.</p>""".stripMargin),
      ("emrConfCmd", "Emr Classification",
        s"""<p>For additional details, see ${htmlLink("Configure Applications", LinkEmrOnEc2ConfigureApps)}
           |in the EMR Documentation.</p>
           |${htmlCodeBlock(sparkRuntime.asEmrClassification, "json")}
           |""".stripMargin),
      ("sparkSubmitCmd", "Spark Submit",
        s"""<p>For additional details, see ${htmlLink("Submitting Applications", LinkSparkSubmit)}
           |in the Spark Documentation.</p>
           |${htmlCodeBlock(sparkRuntime.toSparkSubmit(appInfo), "bash")}
           |""".stripMargin),
    ), "emrOnEc2Cmd", "nav-tabs border-bottom-0 mt-4", "border py-4 px-4 mb-4")

    s"""<!-- Example Commands -->
       |$examplesNavTabs
       |""".stripMargin
  }

  def sparkTab(old: SparkRuntime, optimal: SparkRuntime): String = {

    val sparkTable = htmlTable(
      List("", "Current", "Suggested"),
      List(
        List("Application Runtime", printDurationStr(old.runtime), printDurationStr(optimal.runtime)),
        List("Driver Cores", s"${old.driverCores}", s"${optimal.driverCores}"),
        List("Driver Memory", humanReadableBytes(old.driverMemory), humanReadableBytes(optimal.driverMemory)),
        List("Executor Cores", s"${old.executorCores}", s"${optimal.executorCores}"),
        List("Executor Memory", humanReadableBytes(old.executorMemory), humanReadableBytes(optimal.executorMemory)),
        List("Executors Count", s"${old.executorsNum}", s"${optimal.executorsNum}")
      ), CssTableStyle)

    s"""<!-- Apache Spark -->
       |<div class="app-recommendations spark mt-4 mb-4">
       |  $sparkTable
       |</div>
       |<div class="row mt-4">
       |  <div class="col-sm">
       |    ${if (optimal.runtime >= old.runtime) htmlBoxNote(SparkConfigurationWarning) else ""}
       |  </div>
       |</div>
       |""".stripMargin
  }


  override def render: String = {

    val ec2 = appRecommendations.emrOnEc2Environment.getOrElse(emptyEmrOnEc2)
    val eks = appRecommendations.emrOnEksEnvironment.getOrElse(emptyEmrOnEks)
    val svl = appRecommendations.emrServerlessEnvironment.getOrElse(emptyEmrServerless)
    val current = appRecommendations.currentSparkConf.getOrElse(SparkRuntime.empty)
    val optimal = appRecommendations.optimalSparkConf.getOrElse(SparkRuntime.empty)

    render(ec2, eks, svl, current, optimal)
  }
}
