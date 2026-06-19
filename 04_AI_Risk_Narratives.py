# Databricks notebook source
 import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "uninstall", "anthropic", "-y"], check=True)
subprocess.run([sys.executable, "-m", "pip", "install", "anthropic==0.20.0", "httpx==0.24.1", "--no-deps"], check=True)
print("Anthropic and httpx installed clean")

# COMMAND ----------

# Set Anthropic API key
import anthropic
import os

# Paste your API key here
os.environ["ANTHROPIC_API_KEY"] = "use your key"
client = anthropic.Anthropic()

# Test connection with a simple call
message = client.messages.create(
    model="claude-sonnet-4-6",
    max_tokens=100,
    messages=[
        {"role": "user", "content": "Say: API connection successful"}
    ]
)

print(message.content[0].text)

# COMMAND ----------

# Generate AI risk narratives for each Gold row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import anthropic
import os

# Read Gold table
df_gold = spark.read.table("workspace.gold.lending_club_risk")

# Filter to Fully Paid and Charged Off only
df_gold_filtered = df_gold.filter(
    df_gold.loan_status.isin("Fully Paid", "Charged Off")
)

# Collect to driver — 42 rows, safe to collect
gold_rows = df_gold_filtered.collect()

client = anthropic.Anthropic()

# Generate narrative for each row
results = []
for row in gold_rows:
    prompt = f"""You are a credit risk analyst. Write a 2 sentence risk narrative for this loan segment:
    
Grade: {row['grade']}
Risk Tier: {row['risk_tier']}
Loan Status: {row['loan_status']}
Loan Count: {row['loan_count']}
Avg Loan Amount: ${row['avg_loan_amt']:,.2f}
Avg Interest Rate: {row['avg_interest_rate']}%
Avg DTI: {row['avg_dti']}
Total Volume: ${row['total_loan_volume']:,.2f}

Be specific, use the numbers, and focus on credit risk implications."""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=150,
        messages=[{"role": "user", "content": prompt}]
    )
    
    narrative = message.content[0].text
    results.append((row['grade'], row['loan_status'], row['risk_tier'], narrative))
    print(f"Grade {row['grade']} {row['loan_status']}: done")

print(f"\nTotal narratives generated: {len(results)}")

# COMMAND ----------

# Save narratives as Delta table
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("grade", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("risk_tier", StringType(), True),
    StructField("ai_risk_narrative", StringType(), True)
])

df_narratives = spark.createDataFrame(results, schema)

(df_narratives.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.lending_club_narratives")
)

print("Narratives saved: workspace.gold.lending_club_narratives")
display(df_narratives)

# COMMAND ----------

# Read and display narratives
display(spark.sql("""
    SELECT 
        risk_tier,
        grade,
        loan_status,
        ai_risk_narrative
    FROM workspace.gold.lending_club_narratives
    ORDER BY risk_tier, grade, loan_status
"""))