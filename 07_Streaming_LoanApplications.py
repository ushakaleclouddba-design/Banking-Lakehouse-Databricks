# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.bronze.loan_applications_stream (
    application_id STRING,
    applicant_name STRING,
    loan_amount DOUBLE,
    loan_purpose STRING,
    credit_score INTEGER,
    annual_income DOUBLE,
    dti DOUBLE,
    application_ts TIMESTAMP
) USING DELTA
""")

print("Streaming source table created: workspace.bronze.loan_applications_stream")

# COMMAND ----------

import random
import uuid
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

schema = StructType([
    StructField("application_id", StringType(), True),
    StructField("applicant_name", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("loan_purpose", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("annual_income", DoubleType(), True),
    StructField("dti", DoubleType(), True),
    StructField("application_ts", TimestampType(), True)
])

purposes = ["Home Purchase", "Refinancing", "Debt Consolidation", "Home Improvement", "Auto"]
names = ["John Smith", "Maria Garcia", "Wei Chen", "Priya Patel", "James Wilson",
         "Sarah Johnson", "Ahmed Hassan", "Lisa Brown", "David Kim", "Ana Martinez"]

def generate_applications(batch_size):
    apps = []
    for _ in range(batch_size):
        apps.append((
            str(uuid.uuid4()),
            random.choice(names),
            float(random.randint(5000, 50000) * 100),
            random.choice(purposes),
            int(random.randint(580, 850)),
            float(random.randint(40000, 200000)),
            round(random.uniform(10.0, 45.0), 2),
            datetime.now()
        ))
    return apps

for batch_num in range(1, 6):
    batch = generate_applications(10)
    df_batch = spark.createDataFrame(batch, schema)
    df_batch.write.format("delta").mode("append").saveAsTable("workspace.bronze.loan_applications_stream")
    print("Batch " + str(batch_num) + " written: 10 applications")

print("Total: 50 simulated loan applications written")

# COMMAND ----------

# Streaming simulation using micro-batch pattern
# Demonstrates streaming concept on Serverless compute

from pyspark.sql.functions import col, count, avg, round

print("Starting micro-batch stream simulation...")
print("=" * 50)

# Read all 50 applications
df_all = spark.read.table("workspace.bronze.loan_applications_stream")

# Simulate 5 micro-batches arriving over time
batch_size = 10
total_rows = df_all.count()

for batch_num in range(1, 6):
    rows_so_far = batch_num * batch_size
    
    df_batch = df_all.limit(rows_so_far)
    
    stats = df_batch.groupBy("loan_purpose") \
        .agg(
            count("application_id").alias("count"),
            round(avg("loan_amount"), 2).alias("avg_amount"),
            round(avg("credit_score"), 0).alias("avg_score")
        ).orderBy("count", ascending=False)
    
    print("Micro-batch " + str(batch_num) + " — " + str(rows_so_far) + " applications processed:")
    for row in stats.collect():
        print("  " + row["loan_purpose"] + ": " + str(row["count"]) + " apps, avg $" + str(row["avg_amount"]))
    print("")

print("Stream simulation complete — 50 applications processed in 5 micro-batches")