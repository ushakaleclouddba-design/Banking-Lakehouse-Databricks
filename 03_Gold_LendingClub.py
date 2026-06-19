# Databricks notebook source
# Gold Layer — Risk Analytics Aggregation
# Reads from Silver, aggregates to business-level summary

from pyspark.sql.functions import col, count, avg, sum, round, when

df_silver = spark.read.table("workspace.silver.lending_club_clean")

# Build Gold aggregation — risk summary by grade and loan status
df_gold = df_silver.groupBy("grade", "loan_status") \
    .agg(
        count("id").alias("loan_count"),
        round(avg("loan_amnt"), 2).alias("avg_loan_amt"),
        round(avg("int_rate"), 2).alias("avg_interest_rate"),
        round(avg("annual_inc"), 2).alias("avg_annual_income"),
        round(avg("dti"), 2).alias("avg_dti"),
        round(sum("loan_amnt"), 2).alias("total_loan_volume")
    ) \
    .withColumn("risk_tier",
        when(col("grade").isin("A", "B"), "Low Risk")
        .when(col("grade").isin("C", "D"), "Medium Risk")
        .when(col("grade").isin("E", "F", "G"), "High Risk")
        .otherwise("Unknown")
    ) \
    .orderBy("grade", "loan_status")

print(f"Gold rows: {df_gold.count()}")
display(df_gold)

# COMMAND ----------

# Write Gold Delta table
(df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.lending_club_risk")
)

print("Gold table written: workspace.gold.lending_club_risk")


# COMMAND ----------

# Validate Gold table — full risk picture
display(spark.sql("""
    SELECT 
        risk_tier,
        grade,
        loan_status,
        loan_count,
        avg_loan_amt,
        avg_interest_rate,
        avg_dti,
        total_loan_volume
    FROM workspace.gold.lending_club_risk
    WHERE loan_status IN ('Fully Paid', 'Charged Off')
    ORDER BY risk_tier, grade, loan_status
"""))