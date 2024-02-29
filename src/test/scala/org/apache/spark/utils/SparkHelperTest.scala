package org.apache.spark.utils

import org.scalatest.funsuite.AnyFunSuiteLike

class SparkHelperTest extends AnyFunSuiteLike {

  val TestString1 =
    """org.apache.spark.deploy.yarn.ApplicationMaster --class org.apache.spark.deploy.PythonRunner
      |--primary-py-file test.py --properties-file /......./__spark_conf__/__spark_conf__.properties
      |--dist-cache-conf /....../__spark_conf__/__spark_dist_cache__.properties""".stripMargin

  val TestString2 =
    """org.apache.spark.deploy.SparkSubmit --deploy-mode client
      |--conf spark.driver.bindAddress=192.168.164.195
      |--conf spark.custom.key=value1
      |--properties-file /usr/lib/spark/conf/spark.properties
      |--class com.amazonaws.emr.SparkBenchmark
      |s3://test.bucket/emr-benchmark/cluster-jars/spark-sql-perf.jar param1 param2 param3
      |""".stripMargin

  val TestString3 =
    """org.apache.spark.deploy.yarn.ApplicationMaster
      |--class com.amazonaws.emr.SparkBenchmark
      |--jar s3://test.bucket/emr-benchmark-spark/spark-sql-perf.jar
      |--arg s3://test.bucket/warehouse/tpcds_parquet_3tb_opt
      |--arg s3://test.bucket/emr-benchmark-spark/results/
      |--arg /opt/tpcds-kit/tools
      |--arg parquet
      |--arg 3000
      |--properties-file /.../__spark_conf__/__spark_conf__.properties
      |--dist-cache-conf /.../__spark_conf__/__spark_dist_cache__.properties
      |""".stripMargin

  val TestString4 =
    """org.apache.spark.deploy.SparkSubmit
      |--deploy-mode client
      |--conf spark.driver.bindAddress=192.168.164.195
      |--properties-file /usr/lib/spark/conf/spark.properties
      |--class com.amazonaws.emr.SparkBenchmark
      |s3://test.bucket/emr-benchmark/cluster-jars/spark-sql-perf.jar
      |s3://test.bucket/tpcds-2.13/tpcds_sf1_parquet
      |arg1
      |arg2
      |arg3
      |""".stripMargin

  test("testParseSparkCmd") {
    val parsed1 = SparkHelper.parseSparkCmd(TestString1)
    val parsed2 = SparkHelper.parseSparkCmd(TestString2)
    val parsed3 = SparkHelper.parseSparkCmd(TestString3)
    val parsed4 = SparkHelper.parseSparkCmd(TestString4)

    assert(parsed1.args.isEmpty && parsed1.base.size == 4 && parsed1.conf.isEmpty)
    assert(parsed2.args.size == 4 && parsed2.base.size == 3 && parsed2.conf.size == 2)
    assert(parsed3.args.size == 5 && parsed3.base.size == 4 && parsed3.conf.isEmpty)
    assert(parsed4.args.size == 5 && parsed4.base.size == 3 && parsed4.conf.size == 1)

    assert(parsed1.appScriptJarPath == "test.py")
    assert(parsed2.appScriptJarPath == "s3://test.bucket/emr-benchmark/cluster-jars/spark-sql-perf.jar")
    assert(parsed3.appScriptJarPath == "s3://test.bucket/emr-benchmark-spark/spark-sql-perf.jar")
    assert(parsed4.appScriptJarPath == "s3://test.bucket/emr-benchmark/cluster-jars/spark-sql-perf.jar")

    assert(parsed1.appArguments.isEmpty)
    assert(parsed2.appArguments.size == 3)
    assert(parsed3.appArguments.size == 5)
    assert(parsed4.appArguments.size == 4)
  }

}
