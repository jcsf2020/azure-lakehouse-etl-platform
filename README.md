# Azure Lakehouse ETL Platform

A production-grade data engineering platform built on Azure Databricks, ADLS Gen2, and Azure Data Factory. Implements a full medallion architecture — Bronze → Silver → Gold — for retail and eCommerce analytics, with SQL-driven serving, explicit data quality validation, and formal model contracts.

---

## Platform Overview

The platform ingests raw retail operational data (orders, order items, customers, products, returns), transforms it through structured medallion layers, and delivers analytics-ready tables and views in Databricks SQL via Unity Catalog. All Gold layer objects are defined and executed as SQL DDL assets, not Python transformations.

---

## Architecture

```text
Source Systems (CSV / JSON)
         │
         ▼
┌─────────────────────────┐
│  Azure Data Factory     │  Orchestration, scheduling, pipeline control
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  ADLS Gen2              │  Unified storage — stazlakeetlweu01 (West Europe)
│  Bronze / Silver zones  │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Azure Databricks       │  Compute — PySpark (Bronze→Silver) + SQL (Gold)
│  Unity Catalog          │  Namespace: lakehouse_prod.*
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Gold Layer (SQL)       │  Facts · Dims · Aggregates · DQ · Contracts
│  Databricks SQL         │  Primary serving surface
└─────────────────────────┘
```

---

## Implemented Data Model by Layer

### Bronze — `lakehouse_prod.bronze`

Five raw tables ingested from ADLS Gen2 seed files. Schema is inferred; `_rescued_data` captures malformed fields.

| Table                  | Source Format | Primary Key      |
|------------------------|---------------|------------------|
| `orders_raw`           | JSON          | `order_id`       |
| `order_items_raw`      | JSON          | `order_item_id`  |
| `customers_raw`        | JSON          | `customer_id`    |
| `products_raw`         | JSON          | `product_id`     |
| `returns_raw`          | CSV           | `return_id`      |

Storage path: `abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/`

### Silver — `lakehouse_prod.silver`

Five cleaned tables. Rows with schema violations (`_rescued_data IS NOT NULL`) are excluded. No surrogate keys; natural keys are preserved.

| Table               | Grain                  | Key Transformation            |
|---------------------|------------------------|-------------------------------|
| `orders_clean`      | 1 row per order        | Filter malformed, drop rescue column |
| `order_items_clean` | 1 row per order line   | Filter malformed, drop rescue column |
| `customers_clean`   | 1 row per customer     | Filter malformed, drop rescue column |
| `products_clean`    | 1 row per product      | Filter malformed, drop rescue column |
| `returns_clean`     | 1 row per return event | Filter malformed, drop rescue column |

### Gold — `lakehouse_prod.gold`

SQL-driven. All objects are defined in `sql/gold/`, `sql/dq/`, and `sql/contracts/` and executed in Databricks.

**Fact tables**

| Table                    | Grain                   | Source Join                                      |
|--------------------------|-------------------------|--------------------------------------------------|
| `fact_sales`             | 1 row per `order_item_id` | INNER JOIN `order_items_clean` + `orders_clean` |
| `fact_returns_enriched`  | 1 row per `return_id`   | LEFT JOIN `returns_clean` → `fact_sales`         |

**Dimension tables** (Type 1 — natural key overwrite)

| Table           | Grain                  |
|-----------------|------------------------|
| `dim_customers` | 1 row per `customer_id` |
| `dim_products`  | 1 row per `product_id`  |

**Aggregation tables**

| Table                     | Grain                   | Source               |
|---------------------------|-------------------------|----------------------|
| `orders_channel_summary`  | 1 row per channel       | `orders_clean`       |
| `returns_reason_summary`  | 1 row per return reason | `returns_clean`      |
| `returns_by_product`      | 1 row per product       | `fact_returns_enriched` |

**Views**

| View                 | Description                                              |
|----------------------|----------------------------------------------------------|
| `v_sales_enriched`   | `fact_sales` + `dim_customers` + `dim_products` context  |
| `v_returns_enriched` | `fact_returns_enriched` + `dim_customers` + `dim_products` context |

**Data quality**

| Object          | Type  | Description                                            |
|-----------------|-------|--------------------------------------------------------|
| `dq_summary_v1` | TABLE | One row per check: duplicates, nulls, unmatched returns |
| `v_dq_status`   | VIEW  | PASS / FAIL status per check derived from `dq_summary_v1` |

Validated results: all checks **PASS** except `fact_returns_unmatched_sales = 3`, which is intentional — `fact_returns_enriched` uses a LEFT JOIN to capture returns without a matching sale.

**Model contract**

| Object              | Type  | Description                                             |
|---------------------|-------|---------------------------------------------------------|
| `model_contract_v1` | TABLE | Declared grain, object type, and layer for all Gold objects |

---

## ADF Orchestration

Four pipelines defined in `adf/pipelines/`:

| Pipeline                         | Role                                          |
|----------------------------------|-----------------------------------------------|
| `pl_orchestrate_lakehouse`       | Master — sequential Bronze → Silver → Gold    |
| `pl_bronze_ingestion`            | Triggers Databricks Bronze ingestion notebook |
| `pl_silver_transformations`      | Triggers Silver notebooks (parallel, with dependency ordering) |
| `pl_gold_aggregations`           | Triggers Databricks Gold execution job        |

Silver order items depend on Silver orders and Silver products completing first. All other Silver jobs run in parallel.

---

## Execution Runtime

### Validated path

`scripts/run_full_pipeline.py` is the validated execution harness. It connects to a Databricks SQL Warehouse via environment variables and executes all SQL layers in dependency order — Bronze → Silver → Gold → DQ → Contracts — against the same Unity Catalog namespace used by the ADF path.

```bash
export DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
export DATABRICKS_TOKEN=<pat-or-service-principal-secret>
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx

python scripts/run_full_pipeline.py               # Full run — all layers
python scripts/run_full_pipeline.py --skip-bronze # Silver onward — Bronze already populated
python scripts/run_full_pipeline.py --dry-run     # Validate asset discovery without executing
```

Two parameters control which environment is targeted. Defaults preserve current production behavior (`lakehouse_prod` / `stazlakeetlweu01`) so existing runs are unaffected.

| Variable | Purpose | Default |
|---|---|---|
| `PIPELINE_ENV` | Selects a named block from `config/environments.yml` (`dev` or `prod`) | `prod` |
| `PIPELINE_CATALOG` | Overrides catalog directly | `lakehouse_prod` |
| `PIPELINE_STORAGE_ACCOUNT` | Overrides ADLS Gen2 storage account directly | `stazlakeetlweu01` |

```bash
PIPELINE_ENV=dev python scripts/run_full_pipeline.py   # target lakehouse_dev
```

Each run produces a timestamped artifact directory under `artifacts/execution_runs/<run_id>/` containing a structured `run_log.json` (per-step status, duration, DQ check results, errors) and CSV exports of key Gold tables. The latest successful run is committed in the repository.

The Bronze SQL scripts use the `read_files()` table-valued function — Databricks SQL ingests directly from ADLS Gen2 with no PySpark or Spark cluster required at the Bronze layer.

### Production deployment target

The ADF orchestration documented above is the intended production deployment architecture: ADF schedules and sequences the pipeline, triggering Databricks notebooks for Bronze → Silver transformation and a Databricks SQL job for Gold. The Python library under `src/azure_lakehouse_etl/` implements the PySpark transformation logic for the Bronze → Silver notebooks in that path.

Both paths produce identical objects under `lakehouse_prod.*`. The SQL assets in `sql/gold/`, `sql/dq/`, and `sql/contracts/` are shared. See [`docs/execution-model.md`](docs/execution-model.md) for a full side-by-side comparison, including hardcodes and the promotion roadmap.

---

## Repository Structure

```text
azure-lakehouse-etl-platform/
├── sql/
│   ├── bronze/          # 5 raw ingestion scripts (read_files → Delta)
│   ├── silver/          # 5 cleansed table scripts (CTAS from Bronze)
│   ├── gold/            # 9 objects: facts, dims, aggregates, views
│   ├── dq/              # dq_summary_v1 + v_dq_status
│   └── contracts/       # model_contract_v1
│
├── scripts/
│   └── run_full_pipeline.py   # Validated execution harness (SQL-first runtime)
│
├── artifacts/
│   └── execution_runs/  # Timestamped run logs and CSV exports (latest run committed)
│
├── adf/
│   └── pipelines/       # 4 ADF pipeline definitions (JSON) — production deployment path
│
├── src/
│   └── azure_lakehouse_etl/   # Python transformation library (Bronze→Silver, ADF path)
│
├── databricks/
│   └── notebooks/       # Thin Databricks notebook entrypoints (ADF path)
│
├── tests/
│   └── quality/         # CI smoke tests for Python transformation library
│
├── data_samples/        # Seed data for local development
├── config/              # source_catalog.yml
└── docs/                # Architecture, execution model, data model, pipeline design, runbook
```

---

## Local Validation

```bash
# Install dependencies
uv sync --group dev

# Run smoke tests (Java-dependent tests skip automatically if no JVM is present)
uv run python -m pytest tests/quality -v
```

Smoke tests validate the Python transformation library used for Bronze and Silver processing. Gold layer validation runs in Databricks via `v_dq_status`.

---

## Platform Signals

| Signal | Detail |
|--------|--------|
| SQL-driven Gold layer | All Gold objects defined as SQL DDL — no Python at serving layer |
| Unity Catalog integration | All objects registered under `lakehouse_prod.*` namespace |
| Explicit data quality layer | `dq_summary_v1` + `v_dq_status` — queryable, not just logged |
| Formal model contract | `model_contract_v1` declares grain and object type for every Gold asset |
| Intentional LEFT JOIN semantics | `fact_returns_enriched` preserves unmatched returns by design |
| Azure-native orchestration | ADF controls scheduling, sequencing, and failure handling |
| Medallion architecture — executed | Bronze, Silver, Gold are live Delta tables in Unity Catalog, not a design sketch |
| Validated execution artifacts | `run_full_pipeline.py` proven against Databricks SQL Warehouse; run logs and CSV exports in `artifacts/execution_runs/` |

---

## Why This Project Matters

This platform is built to demonstrate the architecture patterns and execution discipline expected in Azure Databricks data engineering roles — with evidence, not descriptions.

- **SQL-first serving layer.** The entire Gold layer is defined in SQL DDL, not notebooks. A common pattern in Databricks-centric serving layers; keeps the Gold layer declarative, reviewable, and easy to validate.
- **DQ as a pipeline output.** `v_dq_status` runs as part of the pipeline; its results are committed as artifacts. Queryable and provable — not just logged.
- **Formal model contract.** `model_contract_v1` declares grain and object type for every Gold asset. A structured discipline signal, not documentation.
- **Execution evidence committed.** Run logs, DQ status, and Gold table exports are in the repository under `artifacts/execution_runs/`. The platform is validated, not hypothetical.
- **Honest scope.** Seed data, full rebuild semantics, no live ADF deployment. The architecture is correct; the scale is intentionally small. See [`docs/evidence-summary.md`](docs/evidence-summary.md) for a full evidence and limitations summary.
