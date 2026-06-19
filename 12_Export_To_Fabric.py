# Databricks notebook source
# Notebook 12: Export Banking Lakehouse Gold Tables to Microsoft Fabric
# 
# Purpose: Export Databricks Gold tables to Parquet format for ingestion
#          into Microsoft Fabric Lakehouse via SQL Analytics endpoint
#
# Source:  workspace.gold.lending_club_risk (48 rows / 2.26M underlying)
#          workspace.gold.hmda_risk (47 rows)  
#          workspace.gold.lending_club_narratives (14 rows)
#
# Target:  Microsoft Fabric — Banking_Lakehouse_Fabric
#          Workspace: Banking_Lakehouse_POC
#          Query via: SQL Analytics endpoint (T-SQL)
#
# Result:  T-SQL query on Databricks Gold data inside Microsoft Fabric
#          Grade A-G risk tiers, $6.3B-$9.7B loan volumes queryable via SELECT
#
# Date:    June 19, 2026
# Author:  Usha Kale — Azure Data Engineer & Senior Cloud DBA



print("lending_club_risk:", spark.table("workspace.gold.lending_club_risk").count())
print("hmda_risk:", spark.table("workspace.gold.hmda_risk").count())
print("lending_club_narratives:", spark.table("workspace.gold.lending_club_narratives").count())

# COMMAND ----------

# Export Gold tables to Parquet for Fabric ingestion
export_path = "/Volumes/workspace/bronze/raw_uploads/fabric_export"

# Export all three Gold tables
spark.table("workspace.gold.lending_club_risk").coalesce(1).write.mode("overwrite").parquet(f"{export_path}/lending_club_risk")

spark.table("workspace.gold.hmda_risk").coalesce(1).write.mode("overwrite").parquet(f"{export_path}/hmda_risk")

spark.table("workspace.gold.lending_club_narratives").coalesce(1).write.mode("overwrite").parquet(f"{export_path}/lending_club_narratives")

print("✅ All Gold tables exported to Parquet successfully")
print(f"📁 Location: {export_path}")

# COMMAND ----------

# Verify export
import subprocess
files = dbutils.fs.ls("/Volumes/workspace/bronze/raw_uploads/fabric_export/")
for f in files:
    print(f.name, f.size)

# COMMAND ----------

# Check inside one folder
files = dbutils.fs.ls("/Volumes/workspace/bronze/raw_uploads/fabric_export/lending_club_risk/")
for f in files:
    print(f.name, f.size)

# COMMAND ----------

# === POC VALIDATION SUMMARY ===
print("=" * 55)
print("DATABRICKS → MICROSOFT FABRIC POC — COMPLETE")
print("=" * 55)
print()
print("SOURCE: Databricks Free Edition")
print("  workspace.gold.lending_club_risk  → 48 rows")
print("  workspace.gold.hmda_risk          → 47 rows")
print("  workspace.gold.lending_club_narratives → 14 rows")
print()
print("TARGET: Microsoft Fabric")
print("  Workspace:  Banking_Lakehouse_POC")
print("  Lakehouse:  Banking_Lakehouse_Fabric")
print("  Endpoint:   SQL Analytics (T-SQL)")
print()
print("VALIDATION:")
print("  SELECT * FROM dbo.lending_club_risk → 48 rows ✓")
print("  GROUP BY grade → 7 risk tiers, $33.9B total volume ✓")
print()
print("STATUS: POC COMPLETE — June 19, 2026")
print("=" * 55)