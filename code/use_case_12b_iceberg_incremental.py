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
from pyspark.sql.functions import col
import sys

print("Write Storage Location:")
writeLocation = sys.argv[1]
print(writeLocation)

print("Write Tables:")
writeIcebergTableOne = sys.argv[2]
print(writeIcebergTableOne)

# Initialize Spark session with Iceberg
spark = SparkSession.builder \
    .getOrCreate()

# Step 1: Get the snapshot history of the table
history_df = spark.sql("SELECT * FROM {}.history ORDER BY timestamp".format(writeIcebergTableOne))

# Step 2: Extract start and end snapshot IDs (assuming last two snapshots)
snapshots = history_df.orderBy(col("timestamp")).collect()

if len(snapshots) < 2:
    raise Exception("Not enough snapshots to perform incremental read.")

start_snapshot_id = snapshots[-2]["snapshot_id"]
end_snapshot_id = snapshots[-1]["snapshot_id"]

# Step 3: Perform an incremental read using snapshot IDs
incremental_df = spark.read \
    .format("iceberg") \
    .option("start-snapshot-id", start_snapshot_id) \
    .option("end-snapshot-id", end_snapshot_id) \
    .load(writeIcebergTableOne)

# Step 4: Write only the changed rows to S3
incremental_df.write \
    .mode("overwrite") \
    .parquet(writeLocation)

print(f"Incremental changes from snapshot {start_snapshot_id} to {end_snapshot_id} written to S3.")

spark.stop()
