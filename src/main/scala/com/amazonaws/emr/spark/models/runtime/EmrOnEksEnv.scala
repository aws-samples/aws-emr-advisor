package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.Config._
import com.amazonaws.emr.api.AwsCosts.EmrOnEksCost
import com.amazonaws.emr.api.AwsEmr
import com.amazonaws.emr.api.AwsPricing.EmrInstance
import com.amazonaws.emr.report.HtmlBase
import com.amazonaws.emr.spark.models.AppInfo
import com.amazonaws.emr.utils.Constants._
import com.amazonaws.emr.utils.Formatter.{byteStringAsBytes, humanReadableBytes, printDurationStr, toGB}
import software.amazon.awssdk.regions.Region

case class EmrOnEksEnv(
  driverInstance: EmrInstance,
  executorInstance: EmrInstance,
  executorInstanceNum: Int,
  sparkRuntime: SparkRuntime,
  podsPerInstance: Int,
  costs: EmrOnEksCost,
  driver: ResourceRequest,
  executors: ResourceRequest
) extends Ordered[EmrOnEksEnv] with EmrEnvironment with HtmlBase {

  private val nodeMinStorage = byteStringAsBytes(EmrOnEksNodeMinStorage)
  private val nodeSparkStorage = executors.storage * podsPerInstance
  private val sparkStorage = executors.count * executors.storage

  override def label: String = "Emr On Eks"

  override def description: String = "Run your Spark workloads on Amazon EKS"

  override def serviceIcon: String = HtmlSvgEmrOnEks

  def totalCores: Int = driverInstance.vCpu + executorInstanceNum * executorInstance.vCpu

  def totalMemory: Long = byteStringAsBytes(s"${driverInstance.memoryGiB}g") + executorInstanceNum * byteStringAsBytes(s"${executorInstance.memoryGiB}g")

  def totalStorage: Long = sparkStorage + (executorInstanceNum + 1) * nodeMinStorage

  def compare(that: EmrOnEksEnv): Int = this.costs.total compare that.costs.total

  override def instances: List[String] = List(driverInstance.instanceType, executorInstance.instanceType)

  override def htmlDescription: String =
    s"""To optimize performance while minimizing costs, activate ${htmlLink("Karpenter", LinkEmrOnEksKarpenterDoc)}
       |as the default cluster auto-scaler, ensuring that it provisions ${instances.map(htmlBold).mkString(" and ")}
       |instances. Below is a forecast of the nodes that Karpenter will launch.
       |""".stripMargin

  override def htmlServiceNotes: Seq[String] = Seq(
    htmlTextSmall(s"* Costs include ${printDurationStr(EmrOnEksProvisioningMs)} for node provisioning"),
    htmlTextSmall(s"** Storage allocation: ${humanReadableBytes(nodeMinStorage)} (OS) + ${humanReadableBytes(nodeSparkStorage)} (Spark)")
  )

  override def htmlResources: String = htmlTable(
    List("Role", "Count", "Instance", "Cpu", "Memory", s"Storage ${htmlTextSmall("**")}"),
    List(
      List("driver", "1",
        driverInstance.instanceType,
        driverInstance.vCpu.toString,
        s"${driverInstance.memoryGiB}GB",
        s"${humanReadableBytes(nodeMinStorage)}"
      ),
      List("executors", s"$executorInstanceNum",
        executorInstance.instanceType,
        executorInstance.vCpu.toString,
        s"${executorInstance.memoryGiB}GB",
        s"${humanReadableBytes(nodeSparkStorage + nodeMinStorage)}"
      )
    ), "table-bordered table-striped table-sm text-center")

  override def htmlExample(appInfo: AppInfo): String = {
    s"""1. (Optional) Create an ${htmlLink("Amazon EKS cluster with Karpenter", LinkEmrOnEksKarpenterGettingStarted)}
       | and setup an ${htmlLink("EMR on EKS", LinkEmrOnEksQuickStart)} cluster with the ${htmlLink("Spark Operator", LinkEmrOnEksSparkOperator)}
       |<br/><br/>
       |2. Create a Storage class to mount dynamically-created ${htmlLink("persistent volume claim", LinkSparkK8sPvc)} on the Spark executors using ${EbsDefaultStorage.toUpperCase} EBS volumes
       |${htmlCodeBlock(exampleCreateStorageClass, "bash")}
       |3. Create a custom provisioner to scale the cluster
       |${htmlCodeBlock(exampleRequirements, "bash")}
       |4. Review the parameters and submit the application using the Spark Operator
       |${htmlCodeBlock(exampleSubmitJob(appInfo, sparkRuntime), "bash")}
       |<p>For additional details, see ${htmlLink("Running jobs with Amazon EMR on EKS", LinkEmrOnEksJobRunsDoc)}
       |in the EMR Documentation.</p>""".stripMargin
  }

  private def exampleCreateStorageClass: String = {
    s"""cat &lt;&lt;EOF | kubectl apply -n ${htmlTextRed("spark-operator")} -f -
       |apiVersion: storage.k8s.io/v1
       |kind: StorageClass
       |metadata:
       |  name: $EbsDefaultStorage
       |provisioner: kubernetes.io/aws-ebs
       |parameters:
       |  type: $EbsDefaultStorage
       |  fsType: ext4
       |reclaimPolicy: Delete
       |allowVolumeExpansion: true
       |mountOptions:
       |  - debug
       |volumeBindingMode: Immediate
       |EOF
       |""".stripMargin
  }

  private def exampleRequirements: String = {

    val awsRegion = costs.region
    val instanceStr = instances.map(x => s""""$x"""").mkString(",")

    s"""EKS_CLUSTER="${htmlTextRed("EKS_CLUSTER_NAME")}"
       |PROVISIONER_NAME="${htmlTextRed(s"sample-provisioner")}"
       |
       |# create the provisioner with kubectl
       |cat &lt;&lt; EOF | kubectl apply -f -
       |apiVersion: karpenter.sh/v1alpha5
       |kind: Provisioner
       |metadata:
       |  name: $$PROVISIONER_NAME
       |  namespace: karpenter
       |spec:
       |  kubeletConfiguration:
       |    containerRuntime: containerd
       |  requirements:
       |    - key: "topology.kubernetes.io/zone"
       |      operator: In
       |      values: ["${awsRegion}a","${awsRegion}b","${awsRegion}c"]
       |    - key: "karpenter.sh/capacity-type"
       |      operator: In
       |      values: ["spot", "on-demand"]
       |    - key: "node.kubernetes.io/instance-type"
       |      operator: In
       |      values: [$instanceStr]
       |    - key: kubernetes.io/arch
       |      operator: In
       |      values: ["amd64","arm64"]
       |  limits:
       |    resources:
       |      cpu: ${totalCores.toString}
       |  providerRef:
       |    name: $$PROVISIONER_NAME
       |  labels:
       |    type: karpenter
       |    provisioner: $$PROVISIONER_NAME
       |    NodeGroupType: KarpenterSpark
       |  taints:
       |    - key: $$PROVISIONER_NAME
       |      value: 'true'
       |      effect: NoSchedule
       |  ttlSecondsAfterEmpty: 60
       |---
       |apiVersion: karpenter.k8s.aws/v1alpha1
       |kind: AWSNodeTemplate
       |metadata:
       |  name: $$PROVISIONER_NAME
       |  namespace: karpenter
       |spec:
       |  blockDeviceMappings:
       |    - deviceName: /dev/xvda
       |      ebs:
       |        volumeSize: 10Gi
       |        volumeType: gp3
       |        encrypted: true
       |        deleteOnTermination: true
       |  userData: |
       |    #!/bin/bash
       |    chmod +x /usr/bin/setup-local-disks
       |    /usr/bin/setup-local-disks raid0 --update
       |
       |  subnetSelector:
       |    karpenter.sh/discovery: "$$EKS_CLUSTER"
       |  securityGroupSelector:
       |    karpenter.sh/discovery: "$$EKS_CLUSTER"
       |EOF
       |""".stripMargin
  }

  private def exampleSubmitJob(appInfo: AppInfo, conf: SparkRuntime): String = {

    val ecrAccountId = EmrOnEksAccountId(awsRegion.toString)
    val emrRelease = AwsEmr.latestRelease(awsRegion)
    val sparkVersion = AwsEmr.getSparkVersion(emrRelease, awsRegion)
    val epoch = System.currentTimeMillis()

    val sparkCmd = appInfo.sparkCmd.get
    val arguments = if (sparkCmd.appArguments.nonEmpty) {
      s"""\n  arguments: ${sparkCmd.appArguments.map(x => s""""${htmlTextRed(x)}"""").mkString("\n    - ", "\n    - ", "")}"""
    } else ""

    s"""cat &lt;&lt;EOF | kubectl apply -f -
       |apiVersion: "sparkoperator.k8s.io/v1beta2"
       |kind: SparkApplication
       |metadata:
       |  name: ${htmlTextRed(s"spark-test-$epoch")}
       |  namespace: ${htmlTextRed("spark-operator")}
       |spec:
       |  type: ${if (sparkCmd.isScala) s"Scala\n  mainClass: ${htmlTextRed(sparkCmd.appMainClass)}" else "Python\n  pythonVersion: 3"}
       |  mode: cluster
       |  sparkVersion: $sparkVersion
       |  mainApplicationFile: ${htmlTextRed(sparkCmd.appScriptJarPath)}$arguments
       |  image: "$ecrAccountId.dkr.ecr.$awsRegion.amazonaws.com/spark/$emrRelease:latest"
       |  imagePullPolicy: Always
       |  restartPolicy:
       |    type: Never
       |  driver:
       |    serviceAccount: ${htmlTextRed("emr-containers-sa-spark-operator")}
       |    cores: ${driver.cores}
       |    memory: "${toGB(driver.memory)}g"
       |    labels:
       |      NodeGroupType: KarpenterSpark
       |    nodeSelector:
       |      karpenter.sh/capacity-type: on-demand
       |      node.kubernetes.io/instance-type: ${driverInstance.instanceType}
       |    tolerations:
       |      - key: "$$PROVISIONER_NAME"
       |        operator: "Exists"
       |        effect: "NoSchedule"
       |  executor:
       |    cores: ${executors.cores}
       |    memory: "${toGB(executors.memory)}g"
       |    labels:
       |      NodeGroupType: KarpenterSpark
       |    nodeSelector:
       |      karpenter.sh/capacity-type: on-demand
       |      node.kubernetes.io/instance-type: ${executorInstance.instanceType}
       |    tolerations:
       |      - key: "$$PROVISIONER_NAME"
       |        operator: "Exists"
       |        effect: "NoSchedule"
       |  hadoopConf:
       |    # EMRFS filesystem
       |    fs.s3.customAWSCredentialsProvider: com.amazonaws.auth.WebIdentityTokenCredentialsProvider
       |    fs.s3.impl: com.amazon.ws.emr.hadoop.fs.EmrFileSystem
       |    fs.AbstractFileSystem.s3.impl: org.apache.hadoop.fs.s3.EMRFSDelegate
       |    fs.s3.buffer.dir: /mnt/s3
       |
       |    fs.s3.getObject.initialSocketTimeoutMilliseconds: "6000"
       |    fs.s3.metadata.cache.expiration.seconds: "600"
       |    fs.s3.maxConnections: "50000"
       |    fs.s3.threadpool.size: "40"
       |    fs.s3.threadpool.maxSize: "80"
       |    fs.s3.filestatus.metadata.enabled: "true"
       |    fs.s3n.filestatuscache.enable: "true"
       |    mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem: "2"
       |    mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem: "true"
       |
       |  sparkConf:
       |    spark.dynamicAllocation.enabled: "true"
       |    spark.dynamicAllocation.minExecutors: "${executors.count}"
       |    spark.dynamicAllocation.maxExecutors: "${executors.count}"
       |    spark.dynamicAllocation.shuffleTracking.enabled: "true"
       |    spark.kubernetes.driver.request.cores: "${driver.cores - 0.1}"
       |    spark.kubernetes.executor.request.cores: "${executors.cores - 0.1}"
       |
       |    # ephemeral storage
       |    spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName: "OnDemand"
       |    spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass: "$EbsDefaultStorage"
       |    spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit: "${toGB(executors.storage)}Gi"
       |    spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path: "/tmp/data"
       |    spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly: "false"
       |
       |    # Required for EMR Runtime
       |    spark.driver.extraClassPath: /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/home/hadoop/extrajars/*
       |    spark.driver.extraLibraryPath: /usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native
       |    spark.executor.extraClassPath: /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/home/hadoop/extrajars/*
       |    spark.executor.extraLibraryPath: /usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native
       |
       |    spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem: "2"
       |    spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem: "true"
       |    spark.hadoop.mapreduce.output.fs.optimized.committer.enabled: "true"
       |    spark.hadoop.fs.s3.getObject.initialSocketTimeoutMilliseconds: "6000"
       |    spark.sql.parquet.output.committer.class: "com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter"
       |    spark.sql.parquet.fs.optimized.committer.optimization-enabled: "true"
       |    spark.sql.emr.internal.extensions: "com.amazonaws.emr.spark.EmrSparkSessionExtensions"
       |EOF
       |""".stripMargin
  }

}