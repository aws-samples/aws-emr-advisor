package com.amazonaws.emr.utils

import software.amazon.awssdk.regions.Region

import java.util.UUID

object Constants {

  val NotAvailable = "Not Available"

  val DefaultRegion = Region.US_EAST_1.toString

  val LinkEmrAdvisor = "https://github.com/aws-samples/aws-emr-advisor"
  val LinkEmrBestPractice = "https://aws.github.io/aws-emr-best-practices/"

  // Documentation Link
  val LinkEmrOnEc2Documentation = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html"
  val LinkEmrOnEc2ConfigureApps = "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html"
  val LinkEmrOnEc2QuickStart = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html"
  val LinkEmrOnEc2IamRoles = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-service-roles.html"
  val LinkEmrOnEc2TerminationProtection = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/UsingEMR_TerminationProtection.html"
  val LinkEmrOnEc2MultiMaster = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-ha-launch.html"
  val LinkEmrOnEc2AwsTags = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-tags.html"

  def LinkConsoleEc2Subnet(subnetId: String, region: String) =
    s"https://$region.console.aws.amazon.com/vpcconsole/home?region=$region#SubnetDetails:subnetId=$subnetId"

  def LinkConsoleEc2Cluster(clusterId: String, region: String) =
    s"https://$region.console.aws.amazon.com/emr/home?region=$region#/clusterDetails/$clusterId"

  val LinkEmrOnEksDocumentation = "https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks.html"
  val LinkEmrOnEksQuickStart = "https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up.html"
  val LinkEmrOnEksJobRunsDoc = "https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/job-runs-main.html"
  val LinkEmrOnEksKarpenterDoc = "https://karpenter.sh/"
  val LinkEmrOnEksKarpenterGettingStarted = "https://karpenter.sh/docs/getting-started/getting-started-with-karpenter/"
  val LinkEmrOnEksSparkOperator = "https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/spark-operator-setup.html"

  val LinkEmrServerlessDocumentation = "https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html"
  val LinkEmrServerlessQuickStart = "https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html"
  val LinkEmrServerlessArchDoc = "https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/architecture.html"
  val LinkEmrServerlessJobRoleDoc = s"$LinkEmrServerlessQuickStart#gs-runtime-role"

  val LinkSparkQuickStart = "https://spark.apache.org/docs/latest/quick-start.html"
  val LinkSparkConf = "https://spark.apache.org/docs/latest/configuration.html"
  val LinkSparkSubmit = "https://spark.apache.org/docs/latest/submitting-applications.html"
  val LinkSparkK8sPvc = "https://spark.apache.org/docs/3.5.0/running-on-kubernetes.html#using-kubernetes-volumes"

  // Spark Report Application Parameters
  val ParamJar = AppParam("app_jar", "EMR Advisor jar path", isOptional = false)
  val ParamLogs = AppParam("spark_log", "Spark event logs path. Can process files or directories stored in S3, HDFS, or local fs", isOptional = false)
  val ParamBucket = AppParam("bucket", "Amazon S3 bucket to persist HTML reports (e.g my.bucket.name)")
  val ParamDuration = AppParam("duration", "Time duration expected for the job (e.g 15m)")
  val ParamExecutors = AppParam("max-executors", "Maximum number of executors to use in simulations")
  val ParamRegion = AppParam("region", "AWS Region to lookup for costs (e.g. us-east-1)")
  val ParamSpot = AppParam("spot", "Specify spot discount when computing ec2 costs (e.g. 0.7)")

  val SparkAppOptParams = List(ParamBucket, ParamDuration, ParamExecutors, ParamRegion, ParamSpot)
  val SparkAppParams = (ParamJar :: SparkAppOptParams) :+ ParamLogs

  val SparkConfigurationWarning =
    s"""Please note that by default the tool try to find an "acceptable" number of Spark executors trying not to
       |over-subscribe hardware resources, so the estimated runtime might not always be better than your current one.
       |If you want to tune your application to achieve a specific runtime, specify <bold>${ParamDuration.option}</bold>
       | while launching the application.""".stripMargin

  // SVG Icons for services
  def HtmlSvgEmrOnEc2: String = {
    val randomStringId: String = UUID.randomUUID().toString
    s"""
       |  <svg class="w-6 h-6" height="40" width="40" xmlns="http://www.w3.org/2000/svg">
       |    <defs>
       |      <linearGradient x1="0%" y1="100%" x2="100%" y2="0%" id="Arch_Amazon-EC2_${randomStringId}_32_svg__a">
       |        <stop stop-color="#C8511B" offset="0%"></stop>
       |        <stop stop-color="#F90" offset="100%"></stop>
       |      </linearGradient>
       |    </defs>
       |    <g fill="none" fill-rule="evenodd">
       |      <path
       |        d="M0 0h40v40H0z"
       |        fill="url(#Arch_Amazon-EC2_${randomStringId}_32_svg__a)">
       |      </path>
       |      <path d="M26.052 27L26 13.948 13 14v13.052L26.052 27zM27 14h2v1h-2v2h2v1h-2v2h2v1h-2v2h2v1h-2v2h2v1h-2v.052a.95.95 0 01-.948.948H26v2h-1v-2h-2v2h-1v-2h-2v2h-1v-2h-2v2h-1v-2h-2v2h-1v-2h-.052a.95.95 0 01-.948-.948V27h-2v-1h2v-2h-2v-1h2v-2h-2v-1h2v-2h-2v-1h2v-2h-2v-1h2v-.052a.95.95 0 01.948-.948H13v-2h1v2h2v-2h1v2h2v-2h1v2h2v-2h1v2h2v-2h1v2h.052a.95.95 0 01.948.948V14zm-6 19H7V19h2v-1H7.062C6.477 18 6 18.477 6 19.062v13.876C6 33.523 6.477 34 7.062 34h13.877c.585 0 1.061-.477 1.061-1.062V31h-1v2zM34 7.062v13.876c0 .585-.476 1.062-1.061 1.062H30v-1h3V7H19v3h-1V7.062C18 6.477 18.477 6 19.062 6h13.877C33.524 6 34 6.477 34 7.062z"
       |        fill="#FFF">
       |      </path>
       |    </g>
       |  </svg>
       |""".stripMargin
  }

  def HtmlSvgEmrOnEks: String = {
    val randomStringId: String = UUID.randomUUID().toString
    s"""
       |  <svg class="w-6 h-6" height="40" width="40" xmlns="http://www.w3.org/2000/svg">
       |    <defs>
       |      <linearGradient
       |        x1="0%"
       |        y1="100%"
       |        x2="100%"
       |        y2="0%"
       |        id="Arch_Amazon-Elastic-Container-Kubernetes_${randomStringId}_32_svg__a">
       |        <stop stop-color="#C8511B" offset="0%"></stop>
       |        <stop stop-color="#F90" offset="100%"></stop>
       |      </linearGradient>
       |    </defs>
       |    <g fill="none" fill-rule="evenodd">
       |      <path
       |        d="M0 0h40v40H0z"
       |        fill="url(#Arch_Amazon-Elastic-Container-Kubernetes_${randomStringId}_32_svg__a)">
       |      </path>
       |      <path d="M19.403 16.143v3.312l2.87-3.312h1.443l-3.251 4.006 3.417 4.221h-1.423l-3.056-3.805v3.791h-1.286v-8.213h1.286zM32 24.25l-4-2.356v-6.068a.492.492 0 00-.287-.444L22 12.736V8.285l10 4.897V24.25zm.722-11.81l-11-5.387a.504.504 0 00-.485.022.49.49 0 00-.237.417v5.557c0 .19.111.363.287.444L27 16.136v6.035c0 .172.091.332.243.42l5 2.947a.501.501 0 00.757-.42v-12.24a.49.49 0 00-.278-.44zM19.995 32.952L9 27.317V13.169l9-4.849v4.442l-4.746 2.636a.488.488 0 00-.254.427v8.842a.49.49 0 00.258.43l6.5 3.515a.508.508 0 00.482.001l6.25-3.371 3.546 2.33-10.041 5.38zm6.799-8.693a.51.51 0 00-.519-.022L20 27.622l-6-3.245v-8.265l4.746-2.637a.489.489 0 00.254-.427V7.49a.489.489 0 00-.245-.422.512.512 0 00-.496-.01l-10 5.388a.49.49 0 00-.259.43v14.737c0 .184.103.35.268.436l11.5 5.895a.52.52 0 00.471-.005l11-5.895a.486.486 0 00.039-.839l-4.484-2.947z"
       |        fill="#FFF">
       |      </path>
       |    </g>
       |  </svg>
       |""".stripMargin
  }

  def HtmlSvgEmrServerless: String = {
    val randomStringId: String = UUID.randomUUID().toString
    s"""
       |  <svg class="w-6 h-6" height="40" width="40" xmlns="http://www.w3.org/2000/svg">
       |    <defs>
       |      <linearGradient
       |        x1="0%"
       |        y1="100%"
       |        x2="100%"
       |        y2="0%"
       |        id="Arch_Amazon-EMR_${randomStringId}_32_svg__a">
       |        <stop stop-color="#4D27A8" offset="0%"></stop>
       |        <stop stop-color="#A166FF" offset="100%"></stop>
       |      </linearGradient>
       |    </defs>
       |    <g fill="none" fill-rule="evenodd">
       |      <path
       |        d="M0 0h40v40H0z"
       |        fill="url(#Arch_Amazon-EMR_${randomStringId}_32_svg__a)">
       |      </path>
       |      <path
       |        d="M32.011 25.364a2.01 2.01 0 00-.584-1.423 1.984 1.984 0 00-2.82 0 2.025 2.025 0 000 2.844c.753.76 2.067.761 2.82 0 .376-.38.584-.884.584-1.421zm-4.777-1.07c.14-.372.353-.722.646-1.026l-1.058-2.03-1.316 2.507 1.728.55zm-3.159.043l-6.866-2.189a5.531 5.531 0 01-1.405 1.654l3.386 4.737a2.949 2.949 0 012.832-.29l2.053-3.912zm-1.226 6.668a1.993 1.993 0 00-1.981-1.996c-.508 0-1.015.194-1.401.584-.374.378-.58.879-.58 1.412 0 .534.206 1.035.58 1.413.772.778 2.03.778 2.801 0 .375-.378.58-.88.58-1.413zm-10.401-7.027c2.458 0 4.457-2.015 4.457-4.495 0-2.478-2-4.495-4.457-4.495-2.458 0-4.457 2.017-4.457 4.495 0 2.48 2 4.495 4.457 4.495zm6.439-14.983c0 .534.206 1.035.58 1.413.772.779 2.03.779 2.801 0 .375-.378.58-.88.58-1.413a1.993 1.993 0 00-1.98-1.997 1.993 1.993 0 00-1.981 1.997zm3.016 2.801a2.981 2.981 0 01-1.035.195 2.947 2.947 0 01-1.563-.455l-2.974 4.1a5.511 5.511 0 011.321 2.222l6.374-1.985-2.123-4.077zm-4.007 7.687c0 .612-.103 1.197-.286 1.746l6.936 2.21 1.717-3.272-1.768-3.395-6.64 2.068c.024.212.041.426.041.643zm7.562-3.01l1.368 2.625 1.292-2.463c-.07-.06-.145-.108-.212-.175a2.985 2.985 0 01-.453-.609l-1.995.622zm3.149-.72c.753.76 2.067.762 2.82 0 .376-.379.584-.883.584-1.42a2.01 2.01 0 00-.584-1.423 1.984 1.984 0 00-2.82 0 2.025 2.025 0 000 2.844zm3.52 7.482a3.03 3.03 0 010 4.256 2.957 2.957 0 01-2.11.88 2.96 2.96 0 01-2.11-.88 3.01 3.01 0 01-.865-2.21l-2.007-.638-2.18 4.15c.037.034.079.058.114.094.56.565.87 1.318.87 2.118 0 .8-.31 1.554-.87 2.119-.58.584-1.34.876-2.101.876a2.947 2.947 0 01-2.101-.876 2.987 2.987 0 01-.871-2.119c0-.643.208-1.25.577-1.76l-3.5-4.898c-.756.4-1.614.63-2.525.63C9.445 24.977 7 22.513 7 19.483c0-3.028 2.445-5.493 5.448-5.493 1.163 0 2.24.372 3.125 1l2.992-4.124a2.98 2.98 0 01-.669-1.87c0-.801.309-1.553.87-2.12a2.956 2.956 0 014.203 0c.56.567.87 1.319.87 2.12a2.99 2.99 0 01-.87 2.118c-.066.067-.142.117-.213.176l2.232 4.284 2.103-.656a3.028 3.028 0 01.815-2.714 2.97 2.97 0 014.221 0 3.03 3.03 0 010 4.256 2.957 2.957 0 01-2.11.88c-.355 0-.709-.07-1.046-.196l-1.588 3.025 1.309 2.513a2.96 2.96 0 013.435.553zm-19.184-4.251h1.981v.999h-1.98v1.998h-.991v-1.998H9.972v-1h1.98v-1.997h.991v1.998z"
       |        fill="#FFF">
       |      </path>
       |    </g>
       |  </svg>
       |""".stripMargin
  }

}
