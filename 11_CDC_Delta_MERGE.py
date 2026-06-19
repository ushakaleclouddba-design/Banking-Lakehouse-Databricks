# Databricks notebook source
 from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Real IDs from workspace.silver.lending_club_clean
incoming_changes = [
    ("66310712", "Current", "Late (31-120 days)", 35000.0, 14.85),
    ("68356421", "Current", "Charged Off", 22400.0, 12.88),
    ("68426545", "Current", "Late (16-30 days)", 16000.0, 12.88),
    ("68506798", "Current", "Default", 23000.0, 8.49),
    ("68537655", "Current", "Fully Paid", 16800.0, 12.88)
]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("old_status", StringType(), True),
    StructField("new_status", StringType(), True),
    StructField("loan_amnt", DoubleType(), True),
    StructField("int_rate", DoubleType(), True)
])

df_changes = spark.createDataFrame(incoming_changes, schema)

print("Incoming changes: " + str(df_changes.count()) + " records")
display(df_changes)

 

# COMMAND ----------

# Get real IDs from our Silver table to use in CDC simulation
from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "workspace.silver.lending_club_clean")

silver_table.alias("target").merge(
    df_changes.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    set={
        "loan_status": "source.new_status",
        "loan_amnt": "source.loan_amnt",
        "int_rate": "source.int_rate"
    }
).execute()

print("MERGE complete")

# Verify updates
display(spark.sql("""
    SELECT id, loan_status, loan_amnt, int_rate
    FROM workspace.silver.lending_club_clean
    WHERE id IN ('66310712','68356421','68426545','68506798','68537655')
    ORDER BY id DESC
"""))

# COMMAND ----------

# Debug — check exact id values and types in Silver table
display(spark.sql("""
    SELECT id, loan_status, loan_amnt, int_rate,
           length(id) as id_length
    FROM workspace.silver.lending_club_clean
    WHERE id IN ('66310712','68356421','68426545','68506798','68537655')
"""))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY workspace.silver.lending_club_clean LIMIT 5"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Create CDC audit log
df_cdc_log = df_changes.withColumn("change_timestamp", current_timestamp()) \
    .withColumn("operation", lit("UPDATE")) \
    .withColumn("source_system", lit("loan_servicing_feed")) \
    .select(
        "id",
        "old_status",
        "new_status",
        "loan_amnt",
        "int_rate",
        "operation",
        "source_system",
        "change_timestamp"
    )

(df_cdc_log.write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.silver.lending_club_cdc_log")
)

print("CDC log saved: workspace.silver.lending_club_cdc_log")
display(df_cdc_log)

# COMMAND ----------

# Time Travel — query Silver table BEFORE the MERGE
# Version 4 was the last Job run before our MERGE (version 5)

display(spark.sql("""
    SELECT id, loan_status, loan_amnt, int_rate
    FROM workspace.silver.lending_club_clean VERSION AS OF 4
    WHERE id IN ('66310712','68356421','68426545','68506798','68537655')
    ORDER BY id DESC
"""))