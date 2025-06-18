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
from pyspark.sql.functions import rand, col

spark = SparkSession.builder \
    .appName("UseCase14") \
    .getOrCreate()

# Generate large datasets and cache them without unpersisting
for i in range(10):
    print(f"Creating and caching DataFrame {i}")
    df = spark.range(0, 100_000_000).withColumn("rand", rand()) \
        .withColumn("customer_id", rand())# Large DataFrame
    df.cache()
    df.count()  # Force evaluation to cache it
    print(f"DataFrame {i} cached")

"""# Generate synthetic fact data (large dataset)
fact_df = spark.range(0, 100_000_000).toDF("transaction_id") \
    .withColumn("customer_id", (col("transaction_id") % 100000)) \
    .withColumn("amount", (rand() * 1000).cast("double")) \
    .withColumn("region", when(col("transaction_id") % 2 == 0, "US").otherwise("EU"))
fact_df.cache()
fact_df.count()
print("All datasets cached. Done.")

# Generate synthetic dimension data (small dataset)
dim_df = spark.range(0, 1_000_000).toDF("customer_id") \
    .withColumn("customer_type", when(col("customer_id") % 3 == 0, "Gold")
                .when(col("customer_id") % 3 == 1, "Silver")
                .otherwise("Bronze"))

# Shuffle-heavy groupBy BEFORE any filtering
agg_df = fact_df.groupBy("customer_id").agg(
    count("*").alias("txn_count"),
    sum("amount").alias("total_spent")
)"""

def stdev_rand_list(size):
  return size*size*size

spark.udf.register("BUSY_FUNC", stdev_rand_list)

spark.sql('''SELECT *, BUSY_FUNC(r) as r_func
             FROM tuning_demo_input_table
             ORDER BY key''')\
                 .write.mode('overwrite')\
                 .saveAsTable('tuning_demo_output_table')

# Join
spark.sql('''SELECT *, BUSY_FUNC(r) as r_func
             FROM tuning_demo_input_table
             ORDER BY key''').show()
