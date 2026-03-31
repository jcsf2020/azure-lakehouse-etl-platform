# Career Pack — Azure Lakehouse ETL Platform

Reusable career positioning material. All claims are grounded in committed execution evidence.
Update framing per role; the technical facts are stable.

---

## 1. CV Bullets

Select 2–4 per application. Lead with the signal most relevant to the target role.

**Core platform:**
- Designed and validated a SQL-first medallion lakehouse on Azure Databricks (Bronze → Silver → Gold), executing 22 SQL DDL assets against Unity Catalog Delta tables in 47 seconds end-to-end against a live SQL Warehouse.
- Implemented a declarative data quality layer — `dq_summary_v1` (7 checks) and `v_dq_status` (PASS/FAIL view) — executed as part of the pipeline with committed artifacts, not post-hoc validation.
- Authored a formal model contract (`model_contract_v1`) declaring grain and object type for all Gold layer assets; a queryable Delta table, not documentation.

**Orchestration and execution:**
- Built Python execution harness (`run_full_pipeline.py`) for Databricks SQL Warehouse pipeline runs — environment parameterization (`dev`/`prod`), dependency-ordered SQL execution, structured JSON run logging, and CSV artifact export.
- Designed Azure Data Factory orchestration (4 pipelines) for medallion lakehouse sequencing: Bronze ingestion → parallel Silver transformation with dependency ordering → SQL-driven Gold build.

**CI and platform:**
- Established GitHub Actions CI pipeline validating Databricks bundle schema and Python transformation library; no live Databricks credentials required in the CI boundary.

**Compact option (space-constrained CVs):**
- Built Azure Databricks lakehouse platform (Bronze → Silver → Gold, 22 SQL assets, Unity Catalog) with SQL-first Gold layer, declarative DQ, model contracts, ADF orchestration, and validated execution artifacts committed to repository.

---

## 2. LinkedIn Project Bullets

4–6 lines. For use in the Projects or Featured section. Link to the repository.

---

**Azure Lakehouse ETL Platform**

SQL-first medallion lakehouse on Azure Databricks, validated end-to-end against a live Databricks SQL Warehouse.

- 22 SQL DDL assets across Bronze ingestion, Silver cleansing, Gold serving, DQ, and model contracts
- Declarative data quality layer: 7 checks, queryable PASS/FAIL view, execution artifacts committed to repository
- Formal model contract declaring grain and object type for every Gold asset
- Python orchestration harness with dev/prod parameterization, structured run logging, and artifact export
- Azure Data Factory orchestration pipelines for production deployment path
- CI: GitHub Actions (Databricks bundle schema validation + Python smoke tests)

Stack: Azure Databricks · ADLS Gen2 · Unity Catalog · Delta Lake · Azure Data Factory · Databricks SQL · Python · GitHub Actions

---

## 3. 60-Second Interview Pitch

> "I built an end-to-end lakehouse ETL platform on Azure Databricks — full medallion architecture, Bronze through Gold, with SQL-driven serving, an explicit data quality layer, and formal model contracts.
>
> The key architectural decision was keeping Gold entirely in SQL DDL — no Python at the serving layer. A common pattern in Databricks-centric serving layers — it keeps the Gold layer declarative, reviewable, and separate from transformation logic.
>
> I built a Python execution harness that runs all 22 SQL assets against a live SQL Warehouse in dependency order, validates the DQ results, and exports structured artifacts per run. The whole thing completes in under 60 seconds on seed data, and the run logs, DQ status, and table samples are committed to the repository — so the evidence is verifiable.
>
> It's a reference platform, not production — seed data, small scale — but it demonstrates the delivery approach and technical discipline I'd bring to an Azure data engineering engagement."

---

## 4. 2-Minute Technical Walkthrough

Use this for first-round technical screens or take-home walkthrough requests.

**Problem framing (15s):**
> "Retail operations generate data across orders, order items, customers, products, and returns — typically across separate source systems. The platform consolidates these into a single medallion lakehouse on Databricks with a unified analytics layer in Unity Catalog."

**Architecture decision (30s):**
> "I chose a SQL-first approach for Gold — all 9 Gold objects, the DQ table, and the model contract are SQL DDL, not PySpark. For Bronze and Silver, the validated execution path uses `read_files()` in Databricks SQL directly against ADLS Gen2 — no Spark cluster needed. There's also a second execution path using ADF + PySpark notebooks for Bronze/Silver, which is the intended production deployment pattern. Both paths produce identical objects in Unity Catalog. The SQL assets are shared."

**DQ and contracts (20s):**
> "`dq_summary_v1` is a Delta table — a UNION ALL of 7 checks covering nulls, duplicates, and referential integrity. `v_dq_status` maps each check to PASS or FAIL. Both run as part of the pipeline. Separately, `model_contract_v1` is a Delta table that declares the grain and object type for every Gold asset. These are queryable objects you can inspect at any point, not just log output."

**Execution evidence (20s):**
> "The execution harness is `run_full_pipeline.py`. It resolves catalog and storage account from environment variables, executes all 22 SQL files in the correct order, captures per-step timing and status, validates the DQ output, and writes CSVs. The latest run took 47 seconds. The run log, DQ status, and Gold samples are committed in the repo under `artifacts/execution_runs/`."

**Honest bounds (15s):**
> "It's seed data — not production traffic, not benchmarked at scale. There's no incremental loading, no SCD Type 2, no live ADF deployment in CI. The architecture is correct; the scale is small. That's the honest framing."

---

## 5. Recruiter Screen Summary

**For: Azure / Databricks data engineering contract roles**

This project demonstrates end-to-end data engineering on the Azure Databricks stack, covering capabilities relevant to Azure-stack data engineering roles:

- Full medallion lakehouse architecture (Bronze → Silver → Gold) on Azure Databricks and ADLS Gen2
- SQL-driven serving layer via Databricks SQL Warehouse and Unity Catalog — not just PySpark notebooks
- Explicit data quality layer (declarative checks, queryable view, committed execution artifacts)
- Formal model contracts (grain and object type declared for every Gold asset)
- Azure Data Factory orchestration (4 pipelines — master, Bronze, Silver, Gold)
- Python execution harness with environment parameterization and structured run logging
- GitHub Actions CI (bundle schema validation + transformation library smoke tests)

The execution is validated against a live Databricks SQL Warehouse, and the evidence is in the repository: structured run logs, DQ status exports, and Gold table samples. This is a validated reference project built to demonstrate architecture judgment, SQL discipline, and operational awareness in a single provable artifact.

**Most relevant for roles requiring:**
Azure Databricks · Delta Lake · Unity Catalog · Databricks SQL · ADLS Gen2 · Azure Data Factory · medallion / lakehouse architecture · data quality implementation · Python data engineering · SQL-first serving layer design

---

## 6. Career Engine Structured Summary Block

```yaml
title: Azure Lakehouse ETL Platform
type: portfolio_project
status: validated_reference

stack:
  cloud: Azure
  compute: Databricks (SQL Warehouse + PySpark)
  storage: ADLS Gen2 · Delta Lake
  catalog: Unity Catalog
  orchestration: Azure Data Factory
  language: Python 3.11
  ci: GitHub Actions

runtime:
  entry: scripts/run_full_pipeline.py
  mode: SQL-first (22 SQL DDL assets via databricks-sql-connector)
  latest_run: "20260330_222422"
  duration: 46.6s (Silver + Gold, seed data)
  status: SUCCESS

proof:
  - artifacts/execution_runs/20260330_222422/run_log.json
  - artifacts/execution_runs/20260330_222422/dq_status.csv
  - artifacts/execution_runs/20260330_222422/fact_sales_sample.csv
  - CI green (bundle schema + smoke tests)

strengths:
  - SQL-first Gold layer — no Python at serving surface
  - Declarative DQ layer — queryable v_dq_status, committed artifacts
  - Formal model contract — grain and type declared for all Gold objects
  - Environment parameterization — dev/prod via env vars, config file fallback
  - Execution evidence committed to repository
  - Two coherent execution paths — SQL-first (validated) + ADF+notebooks (production target)
  - Separation of concerns — ADF orchestrates, Databricks transforms

known_limits:
  - Seed data only — not production scale or traffic
  - Full rebuild semantics — CREATE OR REPLACE, no MERGE or incremental loading
  - No SCD Type 2
  - No surrogate keys
  - No live ADF deployment in CI
  - No IaC
  - Catalog defaults semi-hardcoded — runtime-parameterizable, not fully multi-tenant

tags:
  - azure-databricks
  - lakehouse
  - medallion-architecture
  - unity-catalog
  - delta-lake
  - azure-data-factory
  - databricks-sql
  - sql-first
  - data-quality
  - model-contracts
  - python-orchestration
  - github-actions-ci
  - adls-gen2
  - contract-market-aligned
```
