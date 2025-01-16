# [WIP] EMR Advisor - EMR on Ec2 Report

This is a Work in Progress and shouldn't be used.

## Remote Development

```bash
# requirements: sshfs & ssh
# mount fs over ssh
emr_cluster=`aws emr list-clusters --active | jq -r .Clusters[0].Id`
emr_master_host=`aws emr describe-cluster --cluster-id $emr_cluster | jq -r .Cluster.MasterPublicDnsName`
mkdir -p /tmp/emr_advisor_cluster 
sshfs hadoop@$emr_master_host:/ /tmp/emr_advisor_cluster
```

## Test

```bash
sbt clean assembly &&  \
java -classpath target/scala-2.12/aws-emr-advisor-assembly-*.jar com.amazonaws.emr.ClusterAnalyzer
```
