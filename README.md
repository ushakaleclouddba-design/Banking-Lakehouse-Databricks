# 🏦 Banking Lakehouse on Databricks

A production-grade Banking Lakehouse framework built on Databricks Free Edition — ingesting 2.26M Lending Club consumer loans and 962K HMDA California mortgage applications through a full Bronze/Silver/Gold medallion architecture with AI-powered risk narrative generation.

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

---

## 📈 Key Findings from the Data

| Finding | Value | Source |
|---------|-------|--------|
| Grade G default rate | 57.6% | Lending Club Gold layer |
| CA home improvement denial rate | 55% | HMDA Gold layer |
| Grade A defaults despite low DTI | $28M | Lending Club Gold layer |
| Bad rows filtered at Silver | 2,783 | Silver validation |

---

## 🚀 Quick Start

### Prerequisites
- Databricks Free Edition account (community.cloud.databricks.com)
- Anthropic API key (for Notebook 04 and 10)
- Lending Club dataset (Kaggle: `accepted_2007_to_2018Q4.csv.gz`)
- HMDA California 2023 dataset (CFPB: `state_CA.csv`)

### Setup
1. Upload datasets to Unity Catalog Volume: `/Volumes/workspace/bronze/raw_uploads/`
2. Import notebooks into your Databricks workspace
3. Set your Anthropic API key as an environment variable:
   ```python
   import os
   api_key = os.environ.get("ANTHROPIC_API_KEY")
   ```
4. Run notebooks in order: 01 → 02 → 03 → 04 → 05 → 06 → 09
5. Or create a Lakeflow Job to orchestrate the full pipeline

### Catalog Structure (Free Edition)
```
workspace/
├── bronze/
│   ├── lending_club_raw
│   └── hmda_raw
├── silver/
│   ├── lending_club_silver
│   └── hmda_silver
└── gold/
    ├── lending_club_gold
    └── hmda_gold
```

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
