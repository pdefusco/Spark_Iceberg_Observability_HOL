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
from pyspark.sql.functions import expr, rand, col
import datetime
import sys

# Parse input
writeIcebergTableOne = sys.argv[1]
writeIcebergTableTwo = sys.argv[2]

# Spark setup
spark = SparkSession.builder \
    .appName("MergeIntoSparkApp1B_DH") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", 64 * 1024 * 1024) \
    .config("spark.sql.shuffle.partitions", "256") \
    .getOrCreate()

NUM_ROWS = 1_000_000_000
SALT_BUCKETS = 16
base_ts = datetime.datetime(2020, 1, 1)

# Generate target DataFrame (df1)
df1 = spark.range(0, NUM_ROWS).toDF("id") \
    .withColumn("category", expr("CASE id % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' WHEN 3 THEN 'D' ELSE 'E' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 100).cast("double")) \
    .withColumn("event_ts", expr(f"date_add(to_date('{base_ts}'), int(id % 30))")) \
    .withColumn("salt", expr(f"id % {SALT_BUCKETS}"))

# Generate source DataFrame (df2) with randomized salt
df2 = spark.range(NUM_ROWS // 2, NUM_ROWS + NUM_ROWS // 2).toDF("id") \
    .withColumn("category", expr("CASE id % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' WHEN 3 THEN 'D' ELSE 'E' END")) \
    .withColumn("value1", (rand() * 1000).cast("double")) \
    .withColumn("value2", (rand() * 100).cast("double")) \
    .withColumn("event_ts", expr(f"date_add(to_date('{base_ts}'), int(id % 30))")) \
    .withColumn("salt", expr(f"CAST(rand() * {SALT_BUCKETS} AS INT)"))

# Drop old tables
spark.sql(f"DROP TABLE IF EXISTS {writeIcebergTableOne}")
spark.sql(f"DROP TABLE IF EXISTS {writeIcebergTableTwo}")

# Create bucketed Iceberg tables on id + salt
spark.sql(f"""
CREATE TABLE {writeIcebergTableOne} (
    id BIGINT,
    category STRING,
    value1 DOUBLE,
    value2 DOUBLE,
    event_ts DATE,
    salt INT
)
USING iceberg
PARTITIONED BY (bucket(256, id), bucket(16, salt))
""")

spark.sql(f"""
CREATE TABLE {writeIcebergTableTwo} (
    id BIGINT,
    category STRING,
    value1 DOUBLE,
    value2 DOUBLE,
    event_ts DATE,
    salt INT
)
USING iceberg
PARTITIONED BY (bucket(256, id), bucket(16, salt))
""")

# Insert salted data
df1.writeTo(writeIcebergTableOne).append()
df2.writeTo(writeIcebergTableTwo).append()

# Salted join via MERGE INTO
spark.sql(f"""
MERGE INTO {writeIcebergTableOne} AS target
USING {writeIcebergTableTwo} AS source
ON target.id = source.id AND target.salt = source.salt
WHEN MATCHED AND source.event_ts > target.event_ts THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

print("Merge with bucketing and salting completed successfully.")
