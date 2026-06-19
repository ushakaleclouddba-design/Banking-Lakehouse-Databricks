# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr, when, round, avg, count, sum

# ============================================
# BRONZE LAYER — Raw ingestion
# ============================================
@dlt.table(
    name="dlt_bronze_loans",
    comment="Raw Lending Club loan data ingested from Volume"
)
def dlt_bronze_loans():
    return (spark.read
        .format("csv")
        .option("header", "true")
        .load("/Volumes/workspace/bronze/raw_uploads/accepted_2007_to_2018Q4.csv.gz")
        .limit(220000)
    )

# ============================================
# SILVER LAYER — Cleaned and filtered
# ============================================
@dlt.table(
    name="dlt_silver_loans",
    comment="Cleaned and typed Lending Club loan data"
)
@dlt.expect("valid_loan_amount", "loan_amnt IS NOT NULL")
@dlt.expect("valid_loan_status", "loan_status IN ('Fully Paid', 'Charged Off', 'Current', 'Late (31-120 days)', 'Late (16-30 days)', 'In Grace Period', 'Default')")
def dlt_silver_loans():
    return (dlt.read("dlt_bronze_loans")
        .select("id", "loan_amnt", "funded_amnt", "term", "int_rate",
                "grade", "sub_grade", "home_ownership", "annual_inc",
                "loan_status", "purpose", "addr_state", "dti", "issue_d")
        .withColumn("loan_amnt", expr("try_cast(loan_amnt as double)"))
        .withColumn("annual_inc", expr("try_cast(annual_inc as double)"))
        .withColumn("dti", expr("try_cast(dti as double)"))
        .withColumn("int_rate", expr("try_cast(regexp_replace(int_rate, '%', '') as double)"))
        .filter(col("loan_status").isin(
            "Fully Paid", "Charged Off", "Current",
            "Late (31-120 days)", "Late (16-30 days)",
            "In Grace Period", "Default"))
        .filter(col("loan_amnt").isNotNull())
    )

# ============================================
# GOLD LAYER — Aggregated risk summary
# ============================================
@dlt.table(
    name="dlt_gold_risk",
    comment="Aggregated loan risk summary by grade and status"
)
def dlt_gold_risk():
    return (dlt.read("dlt_silver_loans")
        .groupBy("grade", "loan_status")
        .agg(
            count("id").alias("loan_count"),
            round(avg("loan_amnt"), 2).alias("avg_loan_amt"),
            round(avg("int_rate"), 2).alias("avg_interest_rate"),
            round(avg("dti"), 2).alias("avg_dti"),
            round(sum("loan_amnt"), 2).alias("total_loan_volume")
        )
        .withColumn("risk_tier",
            when(col("grade").isin("A", "B"), "Low Risk")
            .when(col("grade").isin("C", "D"), "Medium Risk")
            .when(col("grade").isin("E", "F", "G"), "High Risk")
            .otherwise("Unknown")
        )
    )