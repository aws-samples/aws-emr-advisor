# Spark Event Logs Specification

## job_spark_shell
Spark Shell running 2 Spark jobs each with 8 Tasks on a single node with 4 cores per executor. Each job is 
completed in 2 minutes as each task takes 1 minute to complete. Total time for processing is 4 minutes. 
Best possible processing time is 1 minute when all the task can be parallelized. File was generated in this way: 

```bash
spark-shell --conf spark.dynamicAllocation.enabled=false --executor-cores 4 --num-executors 1
```

```scala
val data = sc.parallelize(List(1 to 8), 8)
data.foreachPartition(x=> Thread.sleep(60000))
```