# Databricks notebook source
# Read HMDA California 2023 LAR data
file_path = "/Volumes/workspace/bronze/raw_uploads/state_CA.csv"

df_hmda_raw = spark.read.option("header", "true").csv(file_path)

print(f"Row count: {df_hmda_raw.count()}")
print(f"Column count: {len(df_hmda_raw.columns)}")

# COMMAND ----------

# Write HMDA Bronze Delta table
# Dev subset: 220,000 rows matching Lending Club subset size

df_hmda_bronze = df_hmda_raw.limit(220000)

(df_hmda_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.hmda_raw")
)

print("Bronze table written: workspace.bronze.hmda_raw")
print(f"Rows written: {df_hmda_bronze.count()}")

# COMMAND ----------

# See what HMDA columns look like
display(spark.sql("""
    SELECT 
        action_taken,
        COUNT(*) as application_count,
        loan_purpose,
        loan_type
    FROM workspace.bronze.hmda_raw
    GROUP BY action_taken, loan_purpose, loan_type
    ORDER BY application_count DESC
    LIMIT 10
"""))