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
from pyspark.sql.functions import col, rand, expr, lit, to_timestamp, when
import datetime
import sys

print("Write Tables:")
writeIcebergTableOne = sys.argv[1]
writeIcebergTableTwo = sys.argv[2]
print(writeIcebergTableOne)
print(writeIcebergTableTwo)

# Set up Spark with Iceberg
spark = SparkSession.builder \
    .appName("MergeIntoSparkApp1B") \
    .getOrCreate()

# Parameters
NUM_ROWS = 1_000_000_000
base_ts = datetime.datetime(2020, 1, 1)

# Skewed df1 (target): many rows with id=42
df1 = spark.range(NUM_ROWS).toDF("id") \
    .withColumn("id", when(rand() < 0.95, lit(42)).otherwise(col("id"))) \
    .withColumn("category", expr("CASE id % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' WHEN 3 THEN 'D' ELSE 'E' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 100).cast("double")) \
    .withColumn("event_ts", expr(f"date_add(to_date('{base_ts}'), int(id % 30))"))

# Source df2 (no skew, unique ids)
df2 = spark.range(0, NUM_ROWS).toDF("id") \
    .withColumn("category", expr("CASE id % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' WHEN 3 THEN 'D' ELSE 'E' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 100).cast("double")) \
    .withColumn("event_ts", expr(f"date_add(to_date('{base_ts}'), int(id % 30))"))

# Write target table (Iceberg)
spark.sql("DROP TABLE IF EXISTS {}".format(writeIcebergTableOne))
df1.writeTo(writeIcebergTableOne).using("iceberg").create()

# Write source table (Iceberg)
spark.sql("DROP TABLE IF EXISTS {}".format(writeIcebergTableTwo))
df2.writeTo(writeIcebergTableTwo).using("iceberg").create()

# Perform UPSERT using Iceberg MERGE INTO
spark.sql("""
    MERGE INTO {0} AS target
    USING {1} AS source
    ON target.id = source.id
    WHEN MATCHED AND source.event_ts > target.event_ts THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
""".format(writeIcebergTableOne, writeIcebergTableTwo))

print("Iceberg UPSERT completed using MERGE INTO")

spark.stop()
