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
from pyspark.sql.functions import col, expr, rand
import datetime
import sys

print("Write Tables:")
writeIcebergTableOne = sys.argv[1]
print(writeIcebergTableOne)

# Initialize Spark with Iceberg and Hive catalog
spark = SparkSession.builder \
    .appName("IcebergPartitionEvolutionExample") \
    .getOrCreate()

# Parameters
NUM_ROWS = 1_000_000
BASE_DATE = datetime.datetime(2023, 1, 1)

# Step 1: Generate synthetic data (initial batch)
df_month = spark.range(0, NUM_ROWS).toDF("id") \
    .withColumn("category", expr("CASE id % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 5000).cast("double")) \
    .withColumn("event_ts", expr(f"date_add(to_date('{BASE_DATE}'), int(id % 30))")) \
    .withColumn("month", expr("date_format(event_ts, 'yyyy-MM')")) \
    .withColumn("day", expr("date_format(event_ts, 'yyyy-MM-dd')"))

# Step 2: Create Iceberg table partitioned by month
spark.sql("DROP TABLE IF EXISTS {}".format(writeIcebergTableOne))

df_month.writeTo(writeIcebergTableOne) \
    .using("iceberg") \
    .partitionedBy("month") \
    .create()

# Step 3: Evolve the partition spec to include day instead of month
# Use Iceberg SQL to alter the table
spark.sql("""
    ALTER TABLE {}
    DROP PARTITION FIELD month
""".format(writeIcebergTableOne))
spark.sql("""
    ALTER TABLE {}
    ADD PARTITION FIELD days(event_ts)
""".format(writeIcebergTableOne))

# Step 4: Generate new data for appending using the new partition scheme
df_day = spark.range(NUM_ROWS, NUM_ROWS * 2).toDF("id") \
    .withColumn("category", expr("CASE id % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 5000).cast("double")) \
    .withColumn("event_ts", expr(f"date_add(to_date('{BASE_DATE}'), int(id % 30))")) \
    .withColumn("month", expr("date_format(event_ts, 'yyyy-MM')")) \
    .withColumn("day", expr("date_format(event_ts, 'yyyy-MM-dd')"))

# Append new records (now partitioned by day)
df_day.writeTo(writeIcebergTableOne) \
    .using("iceberg") \
    .append()

# Step 5: Read and show data from the evolved table
df_all = spark.table(writeIcebergTableOne)
df_all.show(10, truncate=False)

spark.stop()
