package com.amazonaws.emr.report.spark

import com.amazonaws.emr.Config.EbsDefaultStorage
import com.amazonaws.emr.utils.Constants._
import com.amazonaws.emr.api.AwsPricing.DefaultCurrency
import com.amazonaws.emr.report.HtmlPage
import com.amazonaws.emr.spark.models.runtime.Environment.emptyEmrOnEc2
import com.amazonaws.emr.spark.models.runtime.Environment.emptyEmrOnEks
import com.amazonaws.emr.spark.models.runtime.Environment.emptyEmrServerless
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.spark.models.AppRecommendations
import com.amazonaws.emr.spark.models.runtime.EmrEnvironment
import com.amazonaws.emr.spark.models.runtime.EmrOnEksEnv
import com.amazonaws.emr.spark.models.runtime.SparkRuntime
import com.amazonaws.emr.report.HtmlReport._
import com.amazonaws.emr.spark.models.OptimalTypes.OptimalType
import com.amazonaws.emr.spark.models.OptimalTypes._
import com.amazonaws.emr.utils.Formatter._

class PageRecommendations(optType: OptimalType, 
                          appInfo: AppInfo,
                          appRecommendations: AppRecommendations) extends HtmlPage {

  override def render: String = {
    
    val current = appRecommendations.currentSparkConf.getOrElse(SparkRuntime.empty)
    val optimal = appRecommendations.sparkConfs.getOrElse(optType, SparkRuntime.empty)
    
    val ec2 = appRecommendations.emrOnEc2Envs.getOrElse(optType, emptyEmrOnEc2)
    val eks = appRecommendations.emrOnEksEnvs.getOrElse(optType, emptyEmrOnEks)
    val svl = appRecommendations.emrServerlessEnvs.getOrElse(optType, emptyEmrServerless)
    
    val addInfo = appRecommendations.additionalInfo.get(optType).map(_.toMap)

    val (deploymentsHtml, suggestedHtml, selectedEnv) = deploymentsTab(optType, ec2, eks, svl, optimal, addInfo)
    val htmlTabPage = htmlNavTabs(id = optType.toString+"recommendedTab", tabs = Seq(
      ("recDeployment"+optType.toString, "Deployments", deploymentsHtml),
      ("recSpark"+optType.toString, "Spark Settings", sparkTab(optType, current, optimal, suggestedHtml)),
      ("recExamples"+optType.toString, "Commands / Examples", exampleTab(optType, ec2, eks, svl, optimal)),
    ), "recDeployment"+optType.toString, "nav-pills border navbar-light bg-light")
    htmlTabPage
  }

  def deploymentsTab(optType: OptimalType,
                     ec2: EmrEnvironment, eks: EmrEnvironment, svl: EmrEnvironment,
                     sparkRuntime: SparkRuntime,
                     additionalInfo: Option[Map[String, String]]): (String, String, EmrEnvironment) = {
    val awsRegion = ec2.costs.region
    val environments = List(
      (ec2.label, ec2.costs.total, ec2),
      (eks.label, eks.costs.total, eks),
      (svl.label, svl.costs.total, svl)
    )
    val cheaper = environments.minBy(_._2)

    val durationStr = additionalInfo.map( m =>
      m.get(ParamDuration.name).map(v => s", expected duration <= ${printDurationStr(v.toLong)},").getOrElse("")
    ).getOrElse("")
    
    val suggested = htmlBoxNote(
      s"""Based on suggested configurations${durationStr} and projected runtime, your application will benefit running within
         | ${htmlBold(cheaper._1)}, with an estimated runtime of ${printDurationStr(sparkRuntime.runtime)} and 
         | total costs of <b>${"%.2f".format(cheaper._2)} $DefaultCurrency</b>
         | in the ${htmlBold(awsRegion)} region""".stripMargin
    )

    (s"""<!-- Emr Deployment Recommended -->
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
       |        ${HtmlSvgEmrOnEc2(optType.toString)}
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
       |        ${HtmlSvgEmrOnEks(optType.toString)}
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
       |        ${HtmlSvgEmrServerless(optType.toString)}
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
       |""".stripMargin, suggested, cheaper._3)
  }

  def exampleTab(optType: OptimalType,
                 ec2: EmrEnvironment, eks: EmrEnvironment, svl: EmrEnvironment, sparkRuntime: SparkRuntime): String = {
    
    val examplesNavTabs = htmlNavTabs(optType.toString + "currEnvExample", Seq(
      (optType.toString + "emrOnEc2Cmd", ec2.label,
        s"""1. (Optional) Create default ${htmlLink("IAM Roles for EMR", LinkEmrOnEc2IamRoles)}
           |${htmlCodeBlock(ec2.exampleCreateIamRoles, "bash")}
           |2. Review the parameters and launch an EC2 cluster to test the configurations
           |${htmlCodeBlock(ec2.exampleSubmitJob(appInfo, sparkRuntime), "bash")}
           |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR", LinkEmrOnEc2QuickStart)}
           |in the AWS Documentation.</p>""".stripMargin),
      (optType.toString + "emrOnEksCmd", eks.label,
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
      (optType.toString + "emrServerlessCmd", svl.label,
        s"""1. (Optional) ${htmlLink("Create an IAM Job Role", LinkEmrServerlessJobRoleDoc)}
           |<br/><br/>
           |2. Create an Emr Serverless Application using the latest EMR release to submit your Spark job
           |${htmlCodeBlock(svl.exampleRequirements(appInfo), "bash")}
           |3. Review the parameters and submit the application
           |${htmlCodeBlock(svl.exampleSubmitJob(appInfo, sparkRuntime), "bash")}
           |<p>For additional details, see ${htmlLink("Getting started with Amazon EMR Serverless", LinkEmrServerlessQuickStart)}
           |in the AWS Documentation.</p>""".stripMargin),
      (optType.toString + "emrConfCmd", "Emr Classification",
        s"""<p>For additional details, see ${htmlLink("Configure Applications", LinkEmrOnEc2ConfigureApps)}
           |in the EMR Documentation.</p>
           |${htmlCodeBlock(sparkRuntime.asEmrClassification, "json")}
           |""".stripMargin),
      (optType.toString + "sparkSubmitCmd", "Spark Submit",
        s"""<p>For additional details, see ${htmlLink("Submitting Applications", LinkSparkSubmit)}
           |in the Spark Documentation.</p>
           |${htmlCodeBlock(sparkRuntime.toSparkSubmit(appInfo), "bash")}
           |""".stripMargin),
    ), optType.toString + "emrOnEc2Cmd", "nav-tabs border-bottom-0 mt-4", "border py-4 px-4 mb-4")

    s"""<!-- Example Commands -->
       |$examplesNavTabs
       |""".stripMargin
  }

  def sparkTab(optType: OptimalType,
               current: SparkRuntime,
               optimal: SparkRuntime,
               suggestedHtml: String
              ): String = {

    val sparkTable = htmlTable(
      List("", "Current", "Suggested"),
      List(
        List("Application Runtime", printDurationStr(current.runtime), printDurationStr(optimal.runtime)),
        List("Driver Cores", s"${current.driverCores}", s"${optimal.driverCores}"),
        List("Driver Memory", humanReadableBytes(current.driverMemory), humanReadableBytes(optimal.driverMemory)),
        List("Executor Cores", s"${current.executorCores}", s"${optimal.executorCores}"),
        List("Executor Memory", humanReadableBytes(current.executorMemory), humanReadableBytes(optimal.executorMemory)),
        List("Max Executors", s"${current.executorsNum}", s"${optimal.executorsNum}")
      ), CssTableStyle)

    s"""<!-- Apache Spark -->
       |<div class="row mt-4">
       |  <div class="col-sm">
       |    $suggestedHtml
       |    ${if (optimal.runtime >= current.runtime) htmlBoxNote(SparkConfigurationWarning) else ""}
       |  </div>
       |</div>
       |<div class="app-recommendations spark mt-4 mb-4">
       |  $sparkTable
       |</div>
       |""".stripMargin
  }
}
