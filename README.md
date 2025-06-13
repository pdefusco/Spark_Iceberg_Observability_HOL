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

* A spark job without Dynamic Allocation where too many executors, cores, and too much memory are set exceeding the cluster’s capacity.

```
cde spark submit code/use_case_3a_overallocating.py \
  --executor-cores 100 \
  --executor-memory "200g"
```

* A spark job without Dynamic Allocation where too many executors, each having just one or two cores, and minimum memory, are set, leading to OOM error.

```
cde spark submit code/use_case_3b_underallocating.py \
  --conf spark.dynamicAllocation.enabled=false \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory "1g"
```

* A Spark job with Dynamic Allocation where max and min executors are set wide, each executor having just one or two cores, and minimum memory, are set, leading to too many small tasks.

```
cde spark submit code/use_case_3c_unordered_shuffles.py \
  --executor-cores 4 \
  --executor-memory "4g"
```

* A Spark job where shuffle partitions property is set too high or too low.

```
cde spark submit code/use_case_3d_high_sp.py \
  --executor-cores 4 \
  --executor-memory "4g"
```
