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
import datetime
import sys
import numpy as np
import random

from dbldatagen import DataGenerator

print("PYTHON EXECUTABLE:", sys.executable)
print("sys.path:", sys.path)
print("Numpy version:", np.__version__)

writeIcebergTableOne = sys.argv[1]
writeIcebergTableTwo = sys.argv[2]

print("Write Tables:")
print(writeIcebergTableOne)
print(writeIcebergTableTwo)

# 1. Start Spark session
spark = SparkSession.builder \
    .appName("MultiKeySkewDynamic") \
    .getOrCreate()

# 2. Generate row count from normal distribution
row_count = int(np.random.normal(loc=500_000_000, scale=50_000_000))  # 5M for safe testing
row_count = max(row_count, 10_000_000)  # Ensure a reasonable floor

print(f"Generating {row_count:,} rows")

# Dynamically choose skewed keys and probabilities for DF1
num_skew_keys_1 = random.randint(5, 10)
skew_keys_1 = random.sample(range(10_000_000, 5_000_000_000), num_skew_keys_1)
skew_probs_1 = np.random.dirichlet(np.ones(num_skew_keys_1), size=1)[0]
skew_values_1 = [str(k) for k in skew_keys_1]
skew_weights_1 = [float(p) for p in skew_probs_1]

# Create weighted distribution for skewed_id
#skew_key_dist_1 = [(str(skew_keys_1[i]), float(skew_probs_1[i])) for i in range(num_skew_keys_1)]
#print(f"Skew key distribution: {skew_key_dist_1}")

# Dynamically choose skewed keys and probabilities for DF1
num_skew_keys_2 = random.randint(5, 10)
skew_keys_2 = random.sample(range(10_000_000, 5_000_000_000), num_skew_keys_2)
skew_probs_2 = np.random.dirichlet(np.ones(num_skew_keys_2), size=1)[0]
skew_values_2 = [str(k) for k in skew_keys_2]
skew_weights_2 = [float(p) for p in skew_probs_2]

# Create weighted distribution for skewed_id
#skew_key_dist_2 = [(str(skew_keys_2[i]), float(skew_probs_2[i])) for i in range(num_skew_keys_2)]
#print(f"Skew key distribution: {skew_key_dist_2}")

base_ts = datetime.datetime(2020, 1, 1)

# 4. Create df2 (non-skewed, full size)
df2_spec = (DataGenerator(spark, name="df2_gen", rows=row_count, partitions=200)
    .withIdOutput()
    .withColumn("skewed_id", "string", values=skew_values_2, weights=skew_weights_2)
    .withColumn("category", "string", values=["A", "B", "C", "D", "E"], random=True)
    .withColumn("value1", "double", minValue=0, maxValue=1000, random=True)
    .withColumn("value2", "double", minValue=0, maxValue=100, random=True)
    .withColumn("value3", "double", minValue=0, maxValue=1000, random=True)
    .withColumn("value4", "double", minValue=0, maxValue=100, random=True)
    .withColumn("value5", "double", minValue=0, maxValue=1000, random=True)
    .withColumn("value6", "double", minValue=0, maxValue=100, random=True)
    .withColumn("value7", "double", minValue=0, maxValue=1000, random=True)
    .withColumn("value8", "double", minValue=0, maxValue=100, random=True)
    .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", interval="1 day", random=True)
)

df2 = df2_spec.build()

# 5. Check if target table exists
table_exists = spark._jsparkSession.catalog().tableExists(writeIcebergTableOne)

if not table_exists:
    print(f"Creating table {writeIcebergTableOne} for the first time.")

    # Create df1 with skewed key
    df1_spec = (DataGenerator(spark, name="df1_gen", rows=row_count, partitions=200)
        .withIdOutput()
        .withColumn("skewed_id", "string", values=skew_values_1, weights=skew_weights_1)
        .withColumn("category", "string", values=["A", "B", "C", "D", "E"], random=True)
        .withColumn("value1", "double", minValue=0, maxValue=1000, random=True)
        .withColumn("value2", "double", minValue=0, maxValue=100, random=True)
        .withColumn("value3", "double", minValue=0, maxValue=1000, random=True)
        .withColumn("value4", "double", minValue=0, maxValue=100, random=True)
        .withColumn("value5", "double", minValue=0, maxValue=1000, random=True)
        .withColumn("value6", "double", minValue=0, maxValue=100, random=True)
        .withColumn("value7", "double", minValue=0, maxValue=1000, random=True)
        .withColumn("value8", "double", minValue=0, maxValue=100, random=True)
        .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", interval="1 day", random=True)
    )

    df1 = df1_spec.build()
    df1.writeTo(writeIcebergTableOne).using("iceberg").create()

else:
    print(f"Table {writeIcebergTableOne} already exists. Skipping creation.")

# 6. Write df2 (source table)
spark.sql(f"DROP TABLE IF EXISTS {writeIcebergTableTwo} PURGE")
df2.writeTo(writeIcebergTableTwo).using("iceberg").create()

# 7. Merge using Iceberg
spark.sql(f"""
    MERGE INTO {writeIcebergTableOne} AS target
    USING {writeIcebergTableTwo} AS source
    ON target.id = source.id
    WHEN MATCHED AND source.event_ts > target.event_ts THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
""")

print("Iceberg UPSERT completed using MERGE INTO")

spark.stop()
