# EMR Advisor - Spark Report

To generate recommendations for your Spark applications, you must collect the corresponding Spark Event Logs for analysis within the tool. For detailed instructions on how to retrieve the Spark Event Logs, refer to [Retrieve Spark Event Logs](https://aws.github.io/aws-emr-best-practices/docs/benchmarks/Analyzing/retrieve_event_logs).

Please note that the tool provides an estimated analysis of your application based on the metrics available in the logs. The results may not always be fully accurate due to variables (primarily hardware factors such as disk throughput, IOPS, network bandwidth, etc.) that cannot be considered when analyzing Spark metrics. For optimal results, ensure that you use only the Spark Event Logs from completed applications.

## Usage

The following is the command syntax with parameters that can be used in the tool:

```bash
Usage: spark-submit --class com.amazonaws.emr.SparkLogsAnalyzer <APP_JAR> [--bucket BUCKET] [--duration DURATION] [--max-executors MAX-EXECUTORS] [--region REGION] [--spot SPOT] <SPARK_LOG>

Parameters:
  APP_JAR                   EMR Advisor jar path
  BUCKET                    Amazon S3 bucket to persist HTML reports (e.g my.bucket.name)
  DURATION                  Time duration expected for the job (e.g 15m)
  MAX-EXECUTORS             Maximum number of executors to use in simulations
  REGION                    AWS Region to lookup for costs (e.g. us-east-1)
  SPOT                      Specify spot discount when computing ec2 costs (e.g. 0.7)
  SPARK_LOG                 Spark event logs path. Can process files or directories stored in S3, HDFS, or local fs
```

When building the project locally, the `APP_JAR` can be found at the following path: `target/SCALA_VERSION/aws-emr-advisor-assembly-RELEASE.jar`.

Please make sure you have the required [IAM Permissions](./IamPermissions.md) when running the application.

### Generate an HTML report for a single Spark event log

#### Generate a report locally

Reports generated locally are saved in the system's `/tmp` directory.

```bash
spark-submit --class com.amazonaws.emr.SparkLogsAnalyzer \
  ./target/scala-2.12/aws-emr-advisor-assembly-*.jar \
  ./src/test/resources/job_spark_pi
```

The exact location of the report is displayed at the end of the analysis in the logs. For example:

```text
25/01/16 14:18:56 INFO AppInsightsAnalyzer: Generate application insights...
Saving html report in /tmp/emr-advisor.spark.1737033541061.html

To access the report, open the following url in your browser:

 - file:///tmp/emr-advisor.spark.1737033541061.html
```

#### Generate a report and store it on Amazon S3

To store the report in an Amazon S3 bucket, specify the `--bucket` parameter. The tool will upload the report to the S3 bucket and generate a pre-signed URL for easy browser access. For example:

```bash
spark-submit --deploy-mode client --class com.amazonaws.emr.SparkLogsAnalyzer \
  ./target/scala-2.12/aws-emr-advisor-assembly-*.jar \
  --bucket YOUR_BUCKET_NAME \
  ./src/test/resources/job_spark_pi
```

Make sure to replace `YOUR_BUCKET_NAME` in the above example.

## Overview - Recommendations

When reviewing the logs, please note that the tool generates three sections within the Recommendation tab, located in the left sidebar of the report, as shown in the figure below:

- **Costs**: The tool generates a configuration that closely matches the current runtime of the submitted application, providing recommendations for a cost-effective EMR deployment.
- **Performance**: The tool suggests a configuration aimed at improving the overall performance and runtime of the application, while selecting the most cost-effective deployment option.
- **(Optional) User Defined**: This section appears only if the user specifies an expected duration using the --duration parameter when analyzing the logs. In this case, the recommendations will be tailored to optimize for the specified duration.


![image info](../docs/images/spark_env_details.png)