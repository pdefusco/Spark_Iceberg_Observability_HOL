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
from pyspark.sql.functions import col, rand, when, expr, sum, count
import sys

print("Write Storage Location:")
writeLocation = sys.argv[1]
print(writeLocation)

# Create SparkSession
spark = SparkSession.builder \
    .appName("UseCase8") \
    .getOrCreate()

# Generate synthetic fact data (large dataset)
fact_df = spark.range(0, 10_000_000).toDF("transaction_id") \
    .withColumn("customer_id", (col("transaction_id") % 100000)) \
    .withColumn("amount", (rand() * 1000).cast("double")) \
    .withColumn("region", when(col("transaction_id") % 2 == 0, "US").otherwise("EU"))

# Generate synthetic dimension data (small dataset)
dim_df = spark.range(0, 100_000_000).toDF("customer_id") \
    .withColumn("customer_type", when(col("customer_id") % 3 == 0, "Gold")
                .when(col("customer_id") % 3 == 1, "Silver")
                .otherwise("Bronze"))

# Step 1: Shuffle-heavy groupBy BEFORE any filtering
agg_df = fact_df.groupBy("customer_id").agg(
    count("*").alias("txn_count"),
    sum("amount").alias("total_spent")
)

# Step 2: Repartition unnecessarily before join
agg_df = agg_df.repartition(500, "customer_id")

# Step 3: Join large datasets before filtering
joined_df = agg_df.join(dim_df, on="customer_id", how="inner")

# Step 4: Filter on region AFTER join (hurts partition pruning)
joined_with_region = joined_df.join(fact_df.select("customer_id", "region"), on="customer_id", how="inner") \
    .filter(col("region") == "US")

# Step 5: Another wide transformation
final_df = joined_with_region.groupBy("region", "customer_type").agg(
    sum("total_spent").alias("region_total_spent")
)

# Step 6: Repartition again before writing (unnecessary)
final_df = final_df.repartition(200)

# Trigger execution
final_df.write.mode("overwrite").parquet(writeLocation)

spark.stop()
