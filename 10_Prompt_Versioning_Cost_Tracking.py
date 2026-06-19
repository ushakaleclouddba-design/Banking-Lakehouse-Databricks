# Databricks notebook source
import subprocess
import sys
subprocess.run([sys.executable, "-m", "pip", "uninstall", "anthropic", "-y"], check=True)
subprocess.run([sys.executable, "-m", "pip", "install", "anthropic==0.20.0", "httpx==0.24.1", "--no-deps"], check=True)
print("Anthropic 0.20.0 + httpx 0.24.1 installed")

# COMMAND ----------

import anthropic
import os
from datetime import datetime

# Set API key
os.environ["ANTHROPIC_API_KEY"] = "your key"

# Prompt Registry — version controlled prompts
PROMPT_REGISTRY = {
    "risk_narrative_v1.0": {
        "version": "1.0",
        "model": "claude-sonnet-4-6",
        "description": "Initial risk narrative prompt — grade + status + metrics",
        "created_date": "2026-06-15",
        "template": """You are a credit risk analyst. Write a 2 sentence risk narrative for this loan segment:

Grade: {grade}
Risk Tier: {risk_tier}
Loan Status: {loan_status}
Loan Count: {loan_count}
Avg Loan Amount: ${avg_loan_amt:,.2f}
Avg Interest Rate: {avg_interest_rate}%
Avg DTI: {avg_dti}
Total Volume: ${total_loan_volume:,.2f}

Be specific, use the numbers, and focus on credit risk implications."""
    },
    "risk_narrative_v1.1": {
        "version": "1.1",
        "model": "claude-sonnet-4-6",
        "description": "Enhanced prompt — adds default rate context and recommendation",
        "created_date": "2026-06-15",
        "template": """You are a senior credit risk officer at a bank. Write a 2 sentence risk narrative for this loan segment. Include a specific risk recommendation.

Grade: {grade}
Risk Tier: {risk_tier}
Loan Status: {loan_status}
Loan Count: {loan_count}
Avg Loan Amount: ${avg_loan_amt:,.2f}
Avg Interest Rate: {avg_interest_rate}%
Avg DTI: {avg_dti}
Total Volume: ${total_loan_volume:,.2f}

Focus on: portfolio exposure, borrower risk profile, and one actionable recommendation."""
    }
}

print("Prompt registry loaded: " + str(len(PROMPT_REGISTRY)) + " versions")
for key in PROMPT_REGISTRY:
    print("  " + key + " — " + PROMPT_REGISTRY[key]["description"])

# COMMAND ----------

# Anthropic pricing (claude-sonnet-4-6 as of 2026)
PRICING = {
    "claude-sonnet-4-6": {
        "input_cost_per_1k": 0.003,
        "output_cost_per_1k": 0.015
    }
}

def call_claude_with_tracking(prompt_key, prompt_variables):
    """
    Calls Claude API with full cost and version tracking.
    Returns narrative + metadata.
    """
    prompt_config = PROMPT_REGISTRY[prompt_key]
    prompt_text = prompt_config["template"].format(**prompt_variables)
    
    client = anthropic.Anthropic()
    
    start_time = datetime.now()
    
    message = client.messages.create(
        model=prompt_config["model"],
        max_tokens=150,
        messages=[{"role": "user", "content": prompt_text}]
    )
    
    end_time = datetime.now()
    duration_ms = (end_time - start_time).total_seconds() * 1000
    
    # Extract usage
    input_tokens = message.usage.input_tokens
    output_tokens = message.usage.output_tokens
    
    # Calculate cost
    pricing = PRICING[prompt_config["model"]]
    input_cost = (input_tokens / 1000) * pricing["input_cost_per_1k"]
    output_cost = (output_tokens / 1000) * pricing["output_cost_per_1k"]
    total_cost = round(input_cost + output_cost, 6)
    
    return {
        "prompt_version": prompt_key,
        "model": prompt_config["model"],
        "narrative": message.content[0].text,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "cost_usd": total_cost,
        "duration_ms": round(duration_ms, 2),
        "timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S")
    }

print("Cost tracking function defined")
print("Pricing loaded: claude-sonnet-4-6 = $0.003/1K input, $0.015/1K output")

# COMMAND ----------

# Test both prompt versions on Grade A Charged Off segment
# This demonstrates prompt versioning — same data, different prompts, compare output quality

test_variables = {
    "grade": "A",
    "risk_tier": "Low Risk",
    "loan_status": "Charged Off",
    "loan_count": 1959,
    "avg_loan_amt": 14454.50,
    "avg_interest_rate": 7.14,
    "avg_dti": 18.16,
    "total_loan_volume": 28316375.00
}

print("Running prompt v1.0...")
result_v1 = call_claude_with_tracking("risk_narrative_v1.0", test_variables)

print("Running prompt v1.1...")
result_v2 = call_claude_with_tracking("risk_narrative_v1.1", test_variables)

# Compare results
print("\n" + "="*60)
print("PROMPT VERSION COMPARISON — Grade A Charged Off")
print("="*60)

print("\nv1.0 Narrative:")
print(result_v1["narrative"])
print("\nTokens: " + str(result_v1["total_tokens"]) + " | Cost: $" + str(result_v1["cost_usd"]) + " | Duration: " + str(result_v1["duration_ms"]) + "ms")

print("\nv1.1 Narrative:")
print(result_v2["narrative"])
print("\nTokens: " + str(result_v2["total_tokens"]) + " | Cost: $" + str(result_v2["cost_usd"]) + " | Duration: " + str(result_v2["duration_ms"]) + "ms")

print("\nCost difference: $" + str(round(result_v2["cost_usd"] - result_v1["cost_usd"], 6)))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("prompt_version", StringType(), True),
    StructField("model", StringType(), True),
    StructField("narrative", StringType(), True),
    StructField("input_tokens", IntegerType(), True),
    StructField("output_tokens", IntegerType(), True),
    StructField("total_tokens", IntegerType(), True),
    StructField("cost_usd", DoubleType(), True),
    StructField("duration_ms", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

rows = [
    (result_v1["prompt_version"], result_v1["model"], result_v1["narrative"],
     result_v1["input_tokens"], result_v1["output_tokens"], result_v1["total_tokens"],
     result_v1["cost_usd"], result_v1["duration_ms"], result_v1["timestamp"]),
    (result_v2["prompt_version"], result_v2["model"], result_v2["narrative"],
     result_v2["input_tokens"], result_v2["output_tokens"], result_v2["total_tokens"],
     result_v2["cost_usd"], result_v2["duration_ms"], result_v2["timestamp"])
]

df_cost_log = spark.createDataFrame(rows, schema)

(df_cost_log.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.prompt_cost_log")
)

print("Cost log saved: workspace.gold.prompt_cost_log")
display(df_cost_log.select("prompt_version", "total_tokens", "cost_usd", "duration_ms"))