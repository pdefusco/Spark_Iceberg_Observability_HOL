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
from pyspark.sql.functions import expr, rand
import datetime
import sys

print("Write Tables:")
writeHiveTableOne = sys.argv[1]
print(writeHiveTableOne)

# Initialize Spark with Hive support
spark = SparkSession.builder \
    .appName("HivePartitionAppendByMonth") \
    .config("spark.sql.shuffle.partitions", "200") \
    .enableHiveSupport() \
    .getOrCreate()

# Parameters
NUM_ROWS = 500_000
BASE_DATE = datetime.datetime(2023, 1, 1)
#TABLE_PATH = "/tmp/hive_partition_append_by_month"

# Step 1: Generate initial dataset
df_initial = spark.range(0, NUM_ROWS).toDF("id") \
    .withColumn("category", expr("CASE id % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 5000).cast("double")) \
    .withColumn("event_ts", expr(f"timestamp('{BASE_DATE}') + interval int(id % 90) days")) \
    .withColumn("month", expr("date_format(event_ts, 'yyyy-MM')"))

# Step 2: Write the initial batch partitioned by month
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
df_initial.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("month") \
    .saveAsTable(writeHiveTableOne)

#    .option("path", TABLE_PATH) \


# Step 3: Generate new batch of data (later range of IDs, same schema)
df_new_batch = spark.range(NUM_ROWS, NUM_ROWS * 2).toDF("id") \
    .withColumn("category", expr("CASE id % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 5000).cast("double")) \
    .withColumn("event_ts", expr(f"timestamp('{BASE_DATE}') + interval int(id % 90) days")) \
    .withColumn("month", expr("date_format(event_ts, 'yyyy-MM')"))

# Step 4: Append the new batch to the same Hive table (partitioned by month)
df_new_batch.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("month") \
    .saveAsTable(writeHiveTableOne)

#    .option("path", TABLE_PATH) \

# Step 5: Show the combined data
spark.table(writeHiveTableOne).show(10, truncate=False)

spark.stop()
