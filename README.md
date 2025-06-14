# Spark & Iceberg Observability Hands on Lab


### Use Case 1: Task Skew

```
cde spark submit code/use_case_1_task_skew.py \
  --executor-memory "2g" \
  --executor-cores 2
```

### Use Case 2: Overcaching

```
cde spark submit code/use_case_2_overcaching.py \
  --executor-cores 2 \
  --executor-memory "4g"
```

### Use Case 3: Misconfigured Resources

A spark job without Dynamic Allocation where too many executors, cores, and too much memory are set exceeding the cluster’s capacity.

```
cde spark submit code/use_case_3_overallocating.py \
  --executor-cores 100 \
  --executor-memory "200g"
```

### Use Case 4: Misconfigured Resources - Underallocating

A spark job without Dynamic Allocation where too many executors, each having just one or two cores, and minimum memory, are set, leading to OOM error.

```
cde spark submit code/use_case_4_underallocating.py \
  --conf spark.dynamicAllocation.enabled=false \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory "1g"
```

### Use Case 5: Misconfigured Resources - Bad Dynamic Allocation

A Spark job with Dynamic Allocation where max and min executors are set wide, each executor having just one or two cores, and minimum memory, are set, leading to too many small tasks.

```
cde spark submit code/use_case_5_unordered_shuffles.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 6: Misconfigured Resources - High Shuffle Partitions

A Spark job where shuffle partitions property is set too high or too low.

```
cde spark submit code/use_case_6_high_sp.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 7: Over Bucketing

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_7_over_bucketing.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 8: Unordered Shuffles

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_8_unordered_shuffles.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 9: Overbroadcasting

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_9_overbroadcasting.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 10: Improper Repartitioning

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_10_improper_repartitoning.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 11a: Hive Upsert

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_11a_hive_upsert.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 11b: Iceberg Merge

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_11b_iceberg_merge.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 12a: Hive Incremental

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_12a_hive_incremental.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 12b: Iceberg Incremental

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_12b_iceberg_incremental.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 13a: Hive Partition Evolution

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_13a_hive_part_evol.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

### Use Case 13b: Iceberg Partition Evolution

A Spark Application written in Spark 2 that has been migrated to Spark 3 is creating thousands of small files when writing, after applying the bucketing operation as it was applied in Spark 2.

```
cde spark submit code/use_case_13b_iceberg_part_evol.py \
  --executor-cores 4 \
  --executor-memory "4g"
```
