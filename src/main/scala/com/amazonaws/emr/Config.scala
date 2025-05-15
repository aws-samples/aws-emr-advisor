package com.amazonaws.emr

object Config {

  val AppName = "EMR Advisor"
  val AppNamePrefix = "emr-advisor"
  val ResourceAppLogoPath = "/logo.png"

  // validity of a s3 pre-signed url in minutes
  // default: 1 day == 1440 minutes
  val S3PreSignedUrlValidity = 1440

  // ===================================================
  // Cluster Advisor Configurations
  // ===================================================
  val EmrExtraInstanceData = "/emr/instance-controller/lib/info/extraInstanceData.json"
  val EmrInstanceControllerInfo = "/emr/instance-controller/lib/info/job-flow.json"

  val EmrLongRunningClusterDaysThreshold: Long = 4L

  // ===================================================
  // Spark Advisor Configurations
  // ===================================================

  // Emr avg. startup times
  val EmrOnEc2ProvisioningMs: Long = 210000 // ca. 3 minutes 30 seconds with only spark installed, no BA
  val EmrOnEksProvisioningMs: Long = 150000 // ca. 2 minutes 30 seconds with Karpenter

  val EbsDefaultStorage = "gp3"

  val EmrOnEc2ClusterMinNodes = 1
  val EmrOnEc2MinStorage = "32g"
  val EmrOnEc2MaxContainersPerInstance = 4
  val EmrOnEc2ReservedOsMemoryGb = "4g"

  val EmrOnEksNodeMinStorage = "10g"
  val EmrOnEksMaxPodsPerInstance = 4
  val EmrOnEksAccountId = Map(
    "ap-northeast-1" -> "059004520145",
    "ap-northeast-2" -> "996579266876",
    "ap-south-1" -> "235914868574",
    "ap-southeast-1" -> "671219180197",
    "ap-southeast-2" -> "038297999601",
    "ca-central-1" -> "351826393999",
    "eu-central-1" -> "107292555468",
    "eu-north-1" -> "830386416364",
    "eu-west-1" -> "483788554619",
    "eu-west-2" -> "118780647275",
    "eu-west-3" -> "307523725174",
    "sa-east-1" -> "052806832358",
    "us-east-1" -> "755674844232",
    "us-east-2" -> "711395599931",
    "us-west-1" -> "608033475327",
    "us-west-2" -> "895885662937"
  )

  val EmrServerlessFreeStorageGb = "20g"
  val EmrServerlessRoleName = "EMRServerlessS3RuntimeRole"

  // vCpu / Memory Thresholds
  val ComputeIntensiveMaxMemory = "2g"
  val MemoryIntensiveMinMemory = "8g"

  val EmrReleaseFilter = List("emr-7.", "emr-8.")

  val SparkMaxDriverCores = 4
  val SparkMaxDriverMemory = "60gb"
  val SparkInstanceFamilies: Seq[String] = List("m", "c", "r", "d")
  val SparkNvmeThreshold = "100g"

  // Executors Simulations

  // time reduction in percentage
  val ExecutorsMaxDropLoss: Double = 5.0
  // Maximum number of simulations for executors
  val ExecutorsMaxTestsCount: Int = 500
  val ExecutorsMaxVisibleTestsCount = 20

}
