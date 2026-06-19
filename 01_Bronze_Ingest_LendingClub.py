# Databricks notebook source
# MAGIC %sql
# MAGIC # Banking Lakehouse — Catalog & Schema Setup
# MAGIC # Free Edition uses 'workspace' catalog (not 'main')
# MAGIC
# MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
# MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
# MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
# MAGIC
# MAGIC print("✅ Schemas created: workspace.bronze, workspace.silver, workspace.gold")

# COMMAND ----------

# Create a Volume to store raw uploaded files
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_uploads")
print("Volume created: workspace.bronze.raw_uploads")

# COMMAND ----------


file_path = "/Volumes/workspace/bronze/raw_uploads/accepted_2007_to_2018Q4.csv.gz"
df_raw = spark.read.option("header", "true").csv(file_path)

print(f"Row count: {df_raw.count()}")
print(f"Column count: {len(df_raw.columns)}")

# COMMAND ----------

file_path = "/Volumes/workspace/bronze/raw_uploads/accepted_2007_to_2018Q4.csv.gz"

df_raw = spark.read.option("header", "true").csv(file_path)

df_bronze = df_raw

(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.lending_club_raw")
)

print("Bronze table written: workspace.bronze.lending_club_raw")
print(f"Rows written: {df_bronze.count()}")

# COMMAND ----------

# Validate Bronze table — query like SQL Server
display(spark.sql("""
    SELECT 
        loan_status,
        COUNT(*) as loan_count,
        ROUND(AVG(CAST(loan_amnt AS DOUBLE)), 2) as avg_loan_amount
    FROM workspace.bronze.lending_club_raw
    GROUP BY loan_status
    ORDER BY loan_count DESC
"""))

# COMMAND ----------

# Validate full scale run
bronze_count = spark.read.table("workspace.bronze.lending_club_raw").count()
silver_count = spark.read.table("workspace.silver.lending_club_clean").count()
gold_count = spark.read.table("workspace.gold.lending_club_risk").count()

print("Full Scale Run Validation:")
print("Bronze: " + str(bronze_count) + " rows")
print("Silver: " + str(silver_count) + " rows")
print("Gold: " + str(gold_count) + " rows")