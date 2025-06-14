#****************************************************************************
# (C) Cloudera, Inc. 2020-2025
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# Create Spark session
spark = SparkSession.builder \
    .appName("BadPartitioningOOM") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Create synthetic large dataset (100 million rows)
df = spark.range(0, 100_000_000).toDF("id") \
    .withColumn("user_id", col("id") % 1000000) \
    .withColumn("amount", (rand() * 100).cast("double")) \
    .withColumn("category", (col("id") % 5))

# Step 1: BAD — Coalesce to a single partition before heavy computation (causes 1 executor to do all the work)
# This forces all data to be processed by a single task => OOM risk
df = df.coalesce(1)

# Step 2: Perform a shuffle-heavy aggregation after collapsing partitions
agg_df = df.groupBy("user_id").sum("amount")

# Step 3: BAD — Repartition to a very high number (causes small files, overhead, task blow-up)
# Each row could be assigned to its own partition — excessive memory and task scheduling overhead
agg_df = agg_df.repartition(100_000, "user_id")

# Step 4: Trigger execution by writing to disk
agg_df.write.mode("overwrite").parquet("/tmp/bad_partitioning_output")

spark.stop()
