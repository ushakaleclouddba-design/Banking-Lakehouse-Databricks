# Databricks notebook source
from pyspark.sql.functions import col, count, avg, sum, round, when, regexp_replace, expr

df_hmda_silver = spark.read.table("workspace.silver.hmda_clean")

# Gold aggregation — mortgage risk summary by loan purpose and action
df_hmda_gold = df_hmda_silver \
    .withColumn("dti_clean", expr("try_cast(regexp_replace(debt_to_income_ratio, '%', '') as double)")) \
    .groupBy("loan_purpose_label", "action_label", "loan_type_label") \
    .agg(
        count("*").alias("application_count"),
        round(avg("loan_amount"), 2).alias("avg_loan_amount"),
        round(avg("interest_rate"), 2).alias("avg_interest_rate"),
        round(avg("income"), 2).alias("avg_income"),
        round(avg("dti_clean"), 2).alias("avg_dti"),
        round(sum("loan_amount"), 2).alias("total_loan_volume")
    ) \
    .withColumn("approval_tier",
        when(col("action_label") == "Originated", "Approved & Funded")
        .when(col("action_label") == "Approved Not Accepted", "Approved Not Taken")
        .when(col("action_label") == "Denied", "Denied")
        .otherwise("Other")
    ) \
    .orderBy("loan_purpose_label", "action_label")

print("Gold rows: " + str(df_hmda_gold.count()))
display(df_hmda_gold)

# COMMAND ----------

# Write HMDA Gold Delta table
(df_hmda_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.hmda_risk")
)

print("Gold table written: workspace.gold.hmda_risk")
print("Rows written: " + str(df_hmda_gold.count()))