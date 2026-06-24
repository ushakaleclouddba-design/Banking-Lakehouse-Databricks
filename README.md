# 🏦 Banking Lakehouse on Databricks

A production-grade Banking Lakehouse framework built on Databricks Free Edition — ingesting 2.26M Lending Club consumer loans and 962K HMDA California mortgage applications through a full Bronze/Silver/Gold medallion architecture with AI-powered risk narrative generation and Microsoft Fabric integration.

---

## 🌟 What This Framework Demonstrates

- End-to-end medallion architecture on Databricks Unity Catalog
- Real banking datasets processed at scale (2.26M + 962K rows)
- AI risk narrative generation using Claude API
- Delta Lake CDC, time travel, and MERGE upsert patterns
- Lakeflow Jobs orchestration with parallel pipeline chains
- Structured Streaming with AvailableNow trigger
- Prompt versioning and API cost tracking
- HMDA fair lending compliance pattern
- **Microsoft Fabric integration — Databricks Gold tables queried via T-SQL SQL Analytics endpoint**

---

## 🏗️ Architecture

```
Source Data (ADLS / Volume Upload)
         |
         v
   Bronze Layer          — Raw ingestion, schema validation, bad row filtering
         |
         v
   Silver Layer          — Cleaned, typed, enriched data
         |
         v
   Gold Layer            — Aggregated, business-ready, interest-rate applied
         |
         v
   AI Risk Narratives    — Claude API generates plain-English risk summaries
         |
         v
   AI/BI Dashboard       — Banking_Lakehouse_Risk_Dashboard (Databricks Genie)
         |
         v
   Microsoft Fabric      — Gold tables exported to Parquet → Fabric Lakehouse
                           Queried via SQL Analytics endpoint (T-SQL)
```

---

## 📁 Repository Structure

```
Banking-Lakehouse-Databricks/
│
├── 01_Bronze_Ingest_LendingClub.py       # Ingest 2.26M loan records to Bronze Delta table
├── 02_Silver_LendingClub.py              # Clean, type-cast, filter bad rows → Silver
├── 03_Gold_LendingClub.py                # Aggregate by grade/segment → Gold
├── 04_AI_Risk_Narratives.py              # Claude API: generate AI risk summaries per segment
├── 05_Bronze_Ingest_HMDA.py              # Ingest 962K HMDA mortgage records to Bronze
├── 06_Silver_HMDA.py                     # Clean HMDA data, handle dash-column names
├── 07_Streaming_LoanApplications.py      # Structured Streaming with AvailableNow trigger
├── 08_DLT_LoanPipeline.py                # Delta Live Tables pipeline (requires classic cluster)
├── 09_Gold_HMDA.py                       # HMDA Gold: denial rates, fair lending signals
├── 10_Prompt_Versioning_Cost_Tracking.py # Claude API prompt versioning + token cost tracking
├── 11_CDC_Delta_MERGE.py                 # Change Data Capture with Delta MERGE upsert
├── 12_Export_To_Fabric.py                # Export Gold tables to Parquet → Microsoft Fabric
│
├── LICENSE                               # MIT License
└── README.md
```

---

## 📊 Datasets

| Dataset | Source | Rows | Columns | Description |
|---------|--------|------|---------|-------------|
| Lending Club Loans | Kaggle | 2,260,701 | 151 | Consumer loans 2007–2018 Q4 |
| HMDA California 2023 | CFPB | 962,000 | 99 | Home mortgage applications |

---

## ⚙️ Pipeline Orchestration

**Lakeflow Job: `JOB_Banking_Lakehouse_Pipeline`**

```
Bronze_LendingClub ──→ Silver_LendingClub ──→ Gold_LendingClub ──→ AI_Risk_Narratives
Bronze_HMDA        ──→ Silver_HMDA        ──→ Gold_HMDA
```

- 7 tasks, two parallel chains
- Full pipeline completes in ~2 min 39 sec on 2.26M rows
- Validated: Bronze 2,260,701 rows → Silver 2,257,918 rows (2,783 bad rows dropped) → Gold 48 segments

---

## 🔗 Microsoft Fabric Integration (Notebook 12)

**POC Complete — June 19, 2026**

Databricks Gold tables exported to Parquet and ingested into Microsoft Fabric Lakehouse, queried via T-SQL SQL Analytics endpoint — zero Spark required.

| Step | Details |
|------|---------|
| Source | Databricks Free Edition — workspace.gold.* |
| Export | Parquet via `coalesce(1).write.parquet()` |
| Target | Microsoft Fabric — Banking_Lakehouse_Fabric |
| Workspace | Banking_Lakehouse_POC (Fabric Trial, East US) |
| Query | SQL Analytics endpoint (T-SQL) |

**Validation Query Results:**

```sql
SELECT grade, risk_tier, 
       SUM(loan_count) as total_loans,
       SUM(total_loan_volume) as total_volume
FROM dbo.lending_club_risk
GROUP BY grade, risk_tier
ORDER BY grade
```

| Grade | Risk Tier | Total Loans | Total Volume |
|-------|-----------|-------------|--------------|
| A | Low Risk | 432,929 | $6.3B |
| B | Low Risk | 663,202 | $9.4B |
| C | Medium Risk | 649,424 | $9.7B |
| D | Medium Risk | 323,733 | $5.1B |
| E | High Risk | 135,103 | $2.4B |
| F | High Risk | 41,553 | $796M |
| G | High Risk | 11,974 | $245M |

---
## 🔐 Unity Catalog Governance POC (June 23, 2026)

**POC Complete — June 23, 2026**

Validated whether Unity Catalog RLS and column masking policies propagate and enforce when Gold Delta tables are queried via Microsoft Fabric SQL Analytics endpoint.

| Test | Result |
|------|--------|
| UC RLS via Fabric SQL Analytics endpoint (OneLake shortcut) | ❌ Bypassed — all 10 rows visible, SSN partially exposed |
| Databricks Materialized Views via OneLake shortcut | ❌ Not accessible — `_enzyme_log` metadata not recognized by Fabric |

**Key Finding:** Fabric reads raw parquet directly from ADLS Gen2 and bypasses the Databricks compute engine entirely. Unity Catalog policies only enforce when queries go through Databricks. For regulated industries — banking, healthcare, financial services — governance controls must be duplicated in both platforms.

**Documentation:** `Appendix_UC_UnityCatalog_Enablement_Playbook_v4.docx`

## 🧠 AI Risk Narratives (Notebook 04)

Uses **Claude API** (Anthropic) to generate plain-English risk summaries for each loan grade segment:

```python
# Example output for Grade G loans
"Grade G borrowers show a 57.6% default rate despite 27.49% average interest rates,
suggesting the pricing model underestimates actual credit risk. Recommend tightening
underwriting criteria and reviewing DTI thresholds for this segment."
```

Features:
- Prompt versioning with version tracking
- Token usage and cost tracking per run
- Anti-hallucination grounding with real data context

---

## 🔑 Key Technical Patterns

| Pattern | Notebook | Description |
|---------|----------|-------------|
| Medallion Architecture | 01–09 | Bronze → Silver → Gold on Unity Catalog |
| Delta CDC + MERGE | 11 | Change Data Capture with MERGE upsert |
| Structured Streaming | 07 | AvailableNow trigger, micro-batch simulation |
| AI Narrative Generation | 04 | Claude API with prompt versioning |
| Cost Tracking | 10 | Token usage per prompt version |
| Fair Lending Signal | 09 | HMDA denial rate analysis by race/income |
| Delta Time Travel | 02, 06 | VERSION AS OF for audit queries |
| Fabric Integration | 12 | Parquet export → Fabric SQL Analytics endpoint |

---

## 📈 Key Findings from the Data

| Finding | Value | Source |
|---------|-------|--------|
| Grade G default rate | 57.6% | Lending Club Gold layer |
| CA home improvement denial rate | 55% | HMDA Gold layer |
| Grade A defaults despite low DTI | $28M | Lending Club Gold layer |
| Bad rows filtered at Silver | 2,783 | Silver validation |
| Total loan portfolio analyzed | $33.9B | Fabric SQL Analytics |

---

## 🚀 Quick Start

### Prerequisites
- Databricks Free Edition account (community.cloud.databricks.com)
- Anthropic API key (for Notebook 04 and 10)
- Lending Club dataset (Kaggle: `accepted_2007_to_2018Q4.csv.gz`)
- HMDA California 2023 dataset (CFPB: `state_CA.csv`)
- Microsoft Fabric trial account (for Notebook 12)

### Setup
1. Upload datasets to Unity Catalog Volume: `/Volumes/workspace/bronze/raw_uploads/`
2. Import notebooks into your Databricks workspace
3. Set your Anthropic API key as an environment variable:
   ```python
   import os
   api_key = os.environ.get("ANTHROPIC_API_KEY")
   ```
4. Run notebooks in order: 01 → 02 → 03 → 04 → 05 → 06 → 09
5. Run Notebook 12 to export to Microsoft Fabric

---

## ⚠️ Known Constraints (Databricks Free Edition)

| Constraint | Workaround |
|-----------|------------|
| No classic cluster | DLT pipeline (Notebook 08) requires classic cluster — code complete but needs paid tier to execute |
| Serverless only | Structured Streaming uses AvailableNow trigger instead of continuous |
| Memory sink unavailable | Micro-batch simulation used instead |
| `try_cast` import | Use `expr("try_cast(...)")` instead of direct import |

---

## 🏦 Banking Domain Context

This framework was built with real banking compliance requirements in mind:

- **HMDA** (Home Mortgage Disclosure Act) — fair lending analysis
- **SOX** — audit trail via Delta transaction log
- **PCI DSS** — data masking patterns in Silver layer
- **FFIEC** — credit risk segmentation in Gold layer

---

## 👤 Author

**Usha Kale**
Azure Data Engineer & Senior Cloud DBA | 15+ years in Banking & Financial Services
- 🌐 [sqlmigrateplus.com](https://sqlmigrateplus.com)
- 💼 [LinkedIn](https://www.linkedin.com/in/usha-kale-56a336a)
- 🐙 [GitHub](https://github.com/ushakaleclouddba-design)

---

## 📜 License

MIT License. See `LICENSE` for details.
