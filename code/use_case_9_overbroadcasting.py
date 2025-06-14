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
from pyspark.sql.functions import broadcast

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UnsafeBroadcastJoinOOM") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \  # Disable automatic broadcast
    .getOrCreate()

# Generate large fact DataFrame (100 million rows)
fact_df = spark.range(0, 100_000_000).toDF("transaction_id") \
    .withColumn("user_id", (col("transaction_id") % 10_000_000)) \
    .withColumn("amount", (rand() * 100).cast("double"))

# Generate "supposedly small" dimension DataFrame (but actually large: 10 million rows)
dim_df = spark.range(0, 10_000_000).toDF("user_id") \
    .withColumn("segment", (col("user_id") % 5)) \
    .withColumn("score", (rand() * 100).cast("double"))

# Developer manually broadcasts dim_df thinking it's small — big mistake!
joined_df = fact_df.join(broadcast(dim_df), on="user_id", how="inner")

# Trigger the join and output (will likely fail due to OOM)
joined_df.write.mode("overwrite").parquet("/tmp/unsafe_broadcast_join_output")

spark.stop()
