ThisBuild / version := "0.3.1"
ThisBuild / organization := "com.amazonaws.emr"
ThisBuild / scalaVersion := "2.12.15"

fork := true
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

lazy val root = (project in file("."))
  .settings(
    name := "aws-emr-advisor"
  )

// Dependencies
val akkaVersion = "2.6.16"
val akkaHttpVersion = "10.2.7"
val apacheHttpVersion = "4.5.13"
// make sure to respect binary compatibility between spark-core and json4s
val awsSdkVersion = "2.20.86"
val json4sVersion = "3.7.0-M11"
val hadoopVersion = "3.3.2"
val sparkVersion = "3.5.3"
val scalaTestsVersion = "3.2.17"

val LibScope = "provided"

libraryDependencies ++= Seq(

  "org.apache.httpcomponents" % "httpmime" % apacheHttpVersion % LibScope,
  "org.apache.httpcomponents" % "httpclient" % apacheHttpVersion % LibScope,

  // Akka
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "org.apache.logging.log4j"  %%  "log4j-api-scala" % "13.0.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0" % Runtime,

  // Spark / Hadoop
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % LibScope,
  "org.apache.spark" %% "spark-core" % sparkVersion % LibScope,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % LibScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % LibScope,

  // AWS Services
  "software.amazon.awssdk" % "ec2" % awsSdkVersion,
  "software.amazon.awssdk" % "emr" % awsSdkVersion,
  "software.amazon.awssdk" % "emrserverless" % awsSdkVersion,
  "software.amazon.awssdk" % "pricing" % awsSdkVersion,
  "software.amazon.awssdk" % "s3" % awsSdkVersion,

  // Json4s
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,

  // Scala Tests
  "org.scalactic" %% "scalactic" % scalaTestsVersion % "test",
  "org.scalatest" %% "scalatest" % scalaTestsVersion % "test"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}