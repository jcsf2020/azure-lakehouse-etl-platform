# Evidence Summary — Azure Lakehouse ETL Platform

Concise executive summary for recruiter, hiring manager, and career-pack use.

---

## Project Purpose

A structured data engineering reference platform demonstrating a full medallion lakehouse on Azure Databricks. Built to validate SQL-first execution against Unity Catalog Delta tables, with explicit data quality gates and formal model contracts.

Target domain: retail and eCommerce analytics — orders, customers, products, returns — from raw CSV/JSON ingestion through to analytics-ready facts, dimensions, and aggregate tables in Databricks SQL.

---

## Validated Runtime

| Item | Detail |
|------|--------|
| Entry point | `scripts/run_full_pipeline.py` |
| Compute | Databricks SQL Warehouse (`databricks-sql-connector`) |
| Execution | 22 SQL DDL assets in dependency order against Unity Catalog |
| Latest run | `20260330_222422` — 46.6 seconds, 17 SQL steps, `SUCCESS` |
| Modes | Full run · skip-bronze · dry-run |
| Parameterization | `PIPELINE_ENV`, `PIPELINE_CATALOG`, `PIPELINE_STORAGE_ACCOUNT` |

Execution evidence is committed to the repository: `artifacts/execution_runs/20260330_222422/` contains `run_log.json` (per-step status, timing, DQ results), `dq_status.csv`, and Gold table exports.

---

## Technical Scope

**Stack:** Azure Databricks · ADLS Gen2 (West Europe) · Azure Data Factory · Unity Catalog · Delta Lake · Databricks SQL Warehouse · Python 3.11 · GitHub Actions CI

**SQL assets — 22 files across 5 layers:**

| Layer | Count | Description |
|-------|-------|-------------|
| Bronze | 5 | `read_files()` → Delta (schema inference + `_rescued_data`) |
| Silver | 5 | CTAS from Bronze; filter `_rescued_data IS NULL` |
| Gold | 9 | 2 fact tables · 2 Type 1 dimensions · 3 aggregation tables · 2 enrichment views |
| DQ | 2 | `dq_summary_v1` (7 checks) · `v_dq_status` (PASS/FAIL view) |
| Contracts | 1 | `model_contract_v1` — declared grain and object type for all Gold objects |

**Python:**
- `run_full_pipeline.py` — execution harness (parameterization, sequencing, artifact export, DQ validation)
- `src/azure_lakehouse_etl/` — PySpark transformation library for the ADF path (Bronze → Silver)
- `tests/quality/` — CI smoke tests

**ADF (production deployment path):**
- 4 pipeline JSON definitions: master orchestrator → Bronze ingestion → Silver (parallel, dependency-ordered) → Gold SQL job

**Databricks bundle:**
- `databricks.yml` with dev/prod target scaffold; CI validates YAML schema without live credentials

---

## Proof Points

| Claim | Evidence |
|-------|----------|
| Executed against live Databricks SQL Warehouse | `run_log.json` — run `20260330_222422`, status `SUCCESS`, 46.6s |
| 17 SQL assets completed without error | Per-step results in `run_log.json` — all `ok` |
| DQ validation produces clean results | 6/7 checks `PASS`; 1 `FAIL` expected by design (`fact_returns_unmatched_sales = 3`, LEFT JOIN semantics) |
| Gold artifacts exported | `fact_sales_sample.csv`, `fact_returns_enriched_sample.csv`, `returns_by_product.csv` committed |
| CI green | GitHub Actions — bundle schema validation + Python smoke tests passing |
| Environment parameterization functional | dev/prod switching via env vars; documented in `docs/execution-model.md` |

---

## Current Limitations — Honest Bounds

- **Not a production system.** Executed against a personal Databricks workspace with seed data. No SLA, no production traffic, no operational monitoring.
- **Seed data only.** Small row counts. Timing reflects small-scale execution, not throughput benchmarking.
- **Full rebuild semantics.** All tables use `CREATE OR REPLACE`. No `MERGE`, no incremental watermarks, no change data capture.
- **No SCD Type 2.** Dimensions use Type 1 overwrite.
- **No surrogate keys.** Natural keys preserved throughout all layers.
- **No live ADF deployment.** Pipeline JSON definitions exist; no deployed trigger in CI.
- **No IaC.** Workspace, cluster, and ADLS provisioning not automated.
- **Semi-hardcoded catalog.** SQL files use `{{catalog}}` resolved at runtime; defaults to `lakehouse_prod`. Full multi-tenant parameterization is not implemented.

---

## Why This Project Is Valuable for B2B Hiring

This platform demonstrates the data engineering discipline that Azure/Databricks-stack contract roles require, with evidence rather than claims:

**SQL-first serving layer.** The entire Gold layer is defined in SQL DDL — no Python at the serving surface. A common pattern in Databricks-centric serving layers; keeps the serving layer declarative, reviewable, and easy to validate.

**DQ as a queryable pipeline output.** `dq_summary_v1` and `v_dq_status` are Delta objects that run as part of the pipeline and whose results are committed as artifacts. The data quality evidence is provable, not described.

**Formal model contract.** `model_contract_v1` declares grain and object type for every Gold asset. A signal of structured modelling discipline.

**Execution evidence committed.** Run logs, DQ status CSV, and Gold table exports are in the repository. A recruiter or technical interviewer can verify the platform ran and what it produced.

**Separation of concerns.** ADF orchestrates. Databricks transforms. Python does not live in the Gold layer. A clearer separation of concerns that makes orchestration, transformation, and serving independently maintainable.

**Operational awareness.** Environment parameterization, idempotent execution, run modes (full / skip-bronze / dry-run), structured artifact logging, and CI discipline show that the design accounts for the operational realities of running a pipeline repeatedly in real environments.
