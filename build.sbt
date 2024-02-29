ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.amazonaws.emr"
ThisBuild / scalaVersion := "2.12.15"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

lazy val root = (project in file("."))
  .settings(
    name := "aws-emr-insights"
  )

// Dependencies
// make sure to respect binary compatibility between spark-core and json4s
val awsSdkVersion = "2.20.86"
val json4sVersion = "3.6.12"
val hadoopVersion = "3.3.2"
val sparkVersion = "3.4.1"
val scalaTestsVersion = "3.2.17"

libraryDependencies ++= Seq(

  "org.apache.httpcomponents" % "httpmime" % "4.5.13" % "provided",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13" % "provided",

  // Spark / Hadoop
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // AWS Services
  "software.amazon.awssdk" % "emr" % awsSdkVersion,
  "software.amazon.awssdk" % "emrserverless" % awsSdkVersion,
  "software.amazon.awssdk" % "pricing" % awsSdkVersion,
  "software.amazon.awssdk" % "s3" % awsSdkVersion,

  // Json4s
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,

  // Scala Tests
  "org.scalactic" %% "scalactic" % scalaTestsVersion,
  "org.scalatest" %% "scalatest" % scalaTestsVersion % "test"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}