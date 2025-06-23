# Spark and Iceberg Observability Hands on Lab

## About the Lab

## About Spark, Iceberg and Cloudera Observability

#### Apache Spark

#### Apache Iceberg

#### About Cloudera Observability

#### About Iceberg Merge Into Statement



## Lab: Tune Iceberg Merge Into Spark Job

### Starting Point

A Spark Application that upserts data into a target table has been rewritten to leverage the Iceberg Merge Into operation. Modify and run the application in CDE or DataHub Spark using any of the following CLI commands.

##### Cloudera Data Engineering

```
cde resource upload --name spark_observability_hol \
  --local-path code/iceberg_merge_original.py

cde job create --name iceberg_merge_original \
  --type spark \
  --application-file use_case_11b_iceberg_merge.py \
  --mount-1-resource spark_observability_hol

cde job run --name use_case_11b_iceberg_merge \
  --executor-cores 4 \
  --executor-memory "4g" \
  --arg spark_catalog.default.iceberg_merge_target_table \
  --arg spark_catalog.default.iceberg_merge_source_table \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=20
```

Before running these modify the job name, source and target table names to reflect your user e.g.

```
cde job create iceberg_merge_original_pauld
[...]
```

and

```
cde job run \
[...]
--arg spark_catalog.default.target_table_pauld \
--arg spark_catalog.default.source_table_pauld
```

##### Cloudera DataHub

```
curl -X POST https://go01-obsr-de-gateway.go01-dem.ylcu-atmi.cloudera.site/go01-obsr-de/cdp-proxy-api/livy_for_spark3/batches \
 -H "Content-Type: application/json" \
 -u pauldefusco:Paolino1987! \
 -d '{
  "file": "/user/pauldefusco/use_case_14_skew_overcaching.py",
  "name": "CDP-Livy-UseCase-14",
  "conf": {
   "spark.dynamicAllocation.enabled": "true",
   "spark.dynamicAllocation.minExecutors": "1",
   "spark.dynamicAllocation.maxExecutors": "20"
  },
  "driverMemory": "4g",
  "executorMemory": "4g",
  "executorCores": 2,
  "numExecutors": 4
 }'
```

##

Navigate to Cloudera Observability and inspect the job runs.

![alt text](img/usecase_11_b_task_skew_1.png)

![alt text](img/usecase_11_b_task_skew_2.png)

![alt text](img/usecase_11_b_task_skew_3.png)

### Use Case 11c: Iceberg Merge Solution

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_11c_iceberg_merge_sol.py \
  --executor-cores 4 \
  --executor-memory "4g" \
  --arg spark_catalog.default.iceberg_merge_target_table \
  --arg spark_catalog.default.iceberg_merge_source_table
```

```
cde resource upload --name spark_observability_hol \
  --local-path code/use_case_11c_iceberg_merge_sol.py

cde job create --name use_case_11c_iceberg_merge_sol \
  --type spark \
  --application-file use_case_11c_iceberg_merge_sol.py \
  --mount-1-resource spark_observability_hol

cde job run --name use_case_11c_iceberg_merge_sol \
  --executor-cores 4 \
  --executor-memory "4g" \
  --arg spark_catalog.default.iceberg_merge_target_table \
  --arg spark_catalog.default.iceberg_merge_source_table \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=20
```
