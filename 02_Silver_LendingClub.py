# Databricks notebook source
# Silver Layer — Read from Bronze
# Select key columns for banking analytics only

df_bronze = spark.read.table("workspace.bronze.lending_club_raw")

df_silver = df_bronze.select(
    "id",
    "loan_amnt",
    "funded_amnt",
    "term",
    "int_rate",
    "grade",
    "sub_grade",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "loan_status",
    "purpose",
    "addr_state",
    "dti",
    "issue_d"
)

print(f"Columns selected: {len(df_silver.columns)}")
print(f"Row count: {df_silver.count()}")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, to_date, expr

# Clean data types and filter bad rows
df_clean = df_silver \
    .withColumn("loan_amnt", expr("try_cast(loan_amnt as double)")) \
    .withColumn("funded_amnt", expr("try_cast(funded_amnt as double)")) \
    .withColumn("annual_inc", expr("try_cast(annual_inc as double)")) \
    .withColumn("dti", expr("try_cast(dti as double)")) \
    .withColumn("int_rate", expr("try_cast(regexp_replace(int_rate, '%', '') as double)")) \
    .withColumn("term_months", expr("try_cast(regexp_replace(term, ' months', '') as integer)")) \
    .withColumn("issue_date", to_date(col("issue_d"), "MMM-yyyy")) \
    .filter(col("loan_status").isin(
        "Fully Paid", "Charged Off", "Current",
        "Late (31-120 days)", "Late (16-30 days)",
        "In Grace Period", "Default"
    )) \
    .filter(col("loan_amnt").isNotNull()) \
    .filter(col("annual_inc").isNotNull())

print(f"Rows after cleaning: {df_clean.count()}")

# COMMAND ----------

# Write Silver Delta table
(df_clean.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.lending_club_clean")
)

print("Silver table written: workspace.silver.lending_club_clean")
print(f"Rows written: {df_clean.count()}")

# COMMAND ----------

# Validate Silver table
display(spark.sql("""
    SELECT 
        grade,
        COUNT(*) as loan_count,
        ROUND(AVG(loan_amnt), 2) as avg_loan_amt,
        ROUND(AVG(int_rate), 2) as avg_interest_rate,
        ROUND(AVG(dti), 2) as avg_dti
    FROM workspace.silver.lending_club_clean
    GROUP BY grade
    ORDER BY grade
"""))