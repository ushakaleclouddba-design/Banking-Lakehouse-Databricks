# Databricks notebook source
# Silver Layer — HMDA
# Read from Bronze and select key mortgage analytics columns

df_hmda_bronze = spark.read.table("workspace.bronze.hmda_raw")

df_hmda_silver = df_hmda_bronze.selectExpr(
    "activity_year",
    "lei",
    "loan_type",
    "loan_purpose",
    "action_taken",
    "loan_amount",
    "interest_rate",
    "loan_term",
    "property_value",
    "income",
    "debt_to_income_ratio",
    "`applicant_race-1` as applicant_race",
    "applicant_sex",
    "applicant_age",
    "county_code"
)

print(f"Columns selected: {len(df_hmda_silver.columns)}")
print(f"Row count: {df_hmda_silver.count()}")

# COMMAND ----------

from pyspark.sql.functions import col, expr, when

df_hmda_clean = df_hmda_silver \
    .withColumn("action_label",
        when(col("action_taken") == "1", "Originated")
        .when(col("action_taken") == "2", "Approved Not Accepted")
        .when(col("action_taken") == "3", "Denied")
        .when(col("action_taken") == "4", "Withdrawn")
        .when(col("action_taken") == "6", "Incomplete")
        .otherwise("Other")
    ) \
    .withColumn("loan_purpose_label",
        when(col("loan_purpose") == "1", "Home Purchase")
        .when(col("loan_purpose") == "2", "Home Improvement")
        .when(col("loan_purpose") == "4", "Refinancing")
        .when(col("loan_purpose") == "31", "Cash-out Refinancing")
        .when(col("loan_purpose") == "32", "Other Refinancing")
        .otherwise("Other")
    ) \
    .withColumn("loan_type_label",
        when(col("loan_type") == "1", "Conventional")
        .when(col("loan_type") == "2", "FHA")
        .when(col("loan_type") == "3", "VA")
        .when(col("loan_type") == "4", "USDA")
        .otherwise("Other")
    ) \
    .withColumn("loan_amount", expr("try_cast(loan_amount as double)")) \
    .withColumn("interest_rate", expr("try_cast(interest_rate as double)")) \
    .withColumn("loan_term", expr("try_cast(loan_term as integer)")) \
    .withColumn("property_value", expr("try_cast(property_value as double)")) \
    .withColumn("income", expr("try_cast(income as double)")) \
    .filter(col("action_taken").isin("1", "2", "3")) \
    .filter(col("loan_amount").isNotNull())

print("Rows after filtering: " + str(df_hmda_clean.count()))

# COMMAND ----------

(df_hmda_clean.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.hmda_clean")
)

print("Silver table written: workspace.silver.hmda_clean")
print("Rows written: " + str(df_hmda_clean.count()))

# COMMAND ----------

display(spark.sql("""
    SELECT 
        loan_purpose_label,
        action_label,
        COUNT(*) as application_count,
        ROUND(AVG(loan_amount), 2) as avg_loan_amount
    FROM workspace.silver.hmda_clean
    GROUP BY loan_purpose_label, action_label
    ORDER BY loan_purpose_label, action_label
"""))