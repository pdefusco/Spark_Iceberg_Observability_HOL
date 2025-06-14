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
from pyspark.sql.functions import col, rand, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BadBucketingApp") \
    .config("spark.sql.sources.bucketing.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Synthetic data (relatively small but spread over many buckets)
df = spark.range(0, 1_000_000).toDF("id") \
    .withColumn("user_id", (col("id") % 100_000)) \
    .withColumn("amount", (rand() * 100).cast("double")) \
    .withColumn("country", when(col("id") % 2 == 0, "US").otherwise("IN"))
    .withColumn("category_index", (col("id") % 20)) \
    .withColumn("category", expr("""
        CASE category_index
            WHEN 0 THEN 'A'
            WHEN 1 THEN 'B'
            WHEN 2 THEN 'C'
            WHEN 3 THEN 'D'
            WHEN 4 THEN 'E'
            WHEN 5 THEN 'F'
            WHEN 6 THEN 'G'
            WHEN 7 THEN 'H'
            WHEN 8 THEN 'I'
            WHEN 9 THEN 'J'
            WHEN 10 THEN 'K'
            WHEN 11 THEN 'L'
            WHEN 12 THEN 'M'
            WHEN 13 THEN 'N'
            WHEN 14 THEN 'O'
            WHEN 15 THEN 'P'
            WHEN 16 THEN 'Q'
            WHEN 17 THEN 'R'
            WHEN 18 THEN 'S'
            WHEN 19 THEN 'T'
            WHEN 20 THEN 'U'
    """))

# Write with bucketing into thousands of small files
# 10,000 buckets on 1 million rows means ~100 rows per bucket
df.write \
    .partitionBy("category") \
    .bucketBy(10_000, "user_id") \
    .sortBy("user_id") \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("bucketed_small_files")

# Show table files location (optional)
print("Wrote to Hive-compatible bucketed table: bucketed_small_files")

spark.stop()
