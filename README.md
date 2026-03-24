# Azure Lakehouse ETL Platform

A production-oriented data engineering portfolio project built around a retail / eCommerce analytics use case. It implements a full medallion architecture (Bronze → Silver → Gold) on Azure Databricks and ADLS Gen2, with a reusable Python library and local smoke-test coverage.

---

## Project Overview

The platform consolidates operational retail data — orders, returns, products, customers, and inventory — into curated analytical tables. The engineering emphasis is on production-minded structuring: a shared transformation library, thin notebook entrypoints, catalog-driven ingestion, and a test suite that runs cleanly in local environments without a live Spark cluster.

---

## Implemented Architecture

```text
Source Systems
      │
      ▼
┌─────────────┐
│   Bronze    │  Raw ingest, stored as-is in Delta Lake
│  (Delta)    │  Catalog-driven — sources defined in YAML
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Silver    │  Cleansed, typed, validated
│  (Delta)    │  6 entities implemented
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Gold     │  Aggregated analytics-ready tables
│  (Delta)    │  3 aggregates implemented
└─────────────┘
```

Storage: ADLS Gen2 | Compute: Azure Databricks | Format: Delta Lake

---

## Current Data Model by Layer

### Bronze

Ingestion is catalog-driven. Sources are declared in a YAML catalog (`source_catalog.py` / `bronze_sources.py`), which drives path generation, table naming, and ingest configuration without per-source boilerplate.

| Source               | Format |
|----------------------|--------|
| orders               | json   |
| order_items          | json   |
| customers            | json   |
| products             | json   |
| returns              | csv    |
| inventory_snapshots  | csv    |

### Silver

Each Silver entity applies type casting, null filtering, business-rule checks, and deduplication before writing to Delta.

| Table                       | Key Transformations                                          |
|-----------------------------|--------------------------------------------------------------|
| `silver.customers`          | Type casting, null filtering, deduplication                  |
| `silver.products`           | Type casting, null filtering, non-negative price checks, deduplication |
| `silver.orders`             | Type casting, null filtering, non-negative amount checks, deduplication |
| `silver.order_items`        | Type casting, null filtering, quantity/value checks, deduplication |
| `silver.returns`            | Type casting, null filtering, non-negative refund checks, deduplication |
| `silver.inventory_snapshots`| Type casting, null filtering, non-negative value checks, deduplication |

### Gold

| Table                    | Grain         | Key Metrics                                                                                      |
|--------------------------|---------------|--------------------------------------------------------------------------------------------------|
| `gold.daily_sales`       | order_date    | total_orders, gross_revenue, total_shipping_amount, total_discount_amount                        |
| `gold.daily_returns`     | return_date   | total_returns, total_refund_amount                                                               |
| `gold.inventory_health`  | snapshot_date | total_products, total_stock_on_hand, total_units_received, total_units_sold, total_units_returned |

---

## Engineering Patterns Used

**Reusable transformation library**
All business logic lives in `src/azure_lakehouse_etl/`. Notebooks import and call functions from this package — they do not contain transformation logic directly.

**Thin notebook entrypoints**
`databricks/notebooks/` contains one notebook per entity per layer. Each notebook is a thin orchestration entrypoint: it sets paths and delegates to the library. This makes logic testable outside of Databricks.

**Catalog-driven Bronze ingestion**
Source definitions are declared once in a YAML catalog. `bronze_sources.py` reads the catalog and produces typed `BronzeSourceConfig` objects, driving ingestion uniformly across all sources.

**Local smoke testing without Java**
`tests/quality/` contains a smoke test per pipeline module. Tests are decorated with `@requires_java` and skip gracefully when no working JVM is present, allowing the full test suite to run in CI and on developer machines without a Spark installation.

---

## Repository Structure

```text
azure-lakehouse-etl-platform/
├── src/
│   └── azure_lakehouse_etl/
│       ├── source_catalog.py           # YAML catalog loader
│       ├── bronze_sources.py           # Source config builder
│       ├── bronze_utils.py             # Shared Bronze helpers
│       ├── silver_customers.py
│       ├── silver_products.py
│       ├── silver_orders.py
│       ├── silver_order_items.py
│       ├── silver_returns.py
│       ├── silver_inventory_snapshots.py
│       ├── gold_daily_sales.py
│       ├── gold_daily_returns.py
│       └── gold_inventory_health.py
│
├── databricks/
│   └── notebooks/
│       ├── bronze/                     # Bronze ingest entrypoint
│       ├── silver/                     # One notebook per Silver entity
│       └── gold/                       # One notebook per Gold aggregate
│
├── tests/
│   └── quality/                        # Smoke tests — one per pipeline module
│
├── data_samples/                       # Seed data for local development
├── config/                             # Environment and pipeline configuration
├── adf/                                # Azure Data Factory pipeline definitions
├── docs/                               # Architecture decisions and design docs
└── pyproject.toml
```

---

## Local Validation

```bash
# Install dependencies
uv sync

# Run smoke tests (Spark-dependent tests skip automatically if Java is unavailable)
pytest tests/quality/ -v
```

Tests validate that transformation functions load correctly, apply expected schema changes, and enforce key field constraints — without requiring a live cluster.

---

## Why This Project Is Recruiter-Relevant

| Signal | Detail |
|--------|--------|
| Medallion architecture | Full Bronze → Silver → Gold implemented, not just sketched |
| Library-first design | Transformation logic is importable, testable, and notebook-agnostic |
| Catalog-driven ingestion | New sources are added via config, not new code |
| Local testability | Smoke tests run without Spark; CI-friendly from the start |
| Production structuring | Separation of concerns between library, entrypoints, and tests |
| Realistic use case | Retail analytics domain with meaningful aggregations |

---

## Current Status

Active development. The following layers are fully implemented and tested locally:

- **Bronze** — catalog-driven ingestion complete
- **Silver** — all 6 entities implemented in final pattern
- **Gold** — 3 aggregates implemented (daily_sales, daily_returns, inventory_health)

Cloud deployment (ADF orchestration, ADLS Gen2 integration, Databricks job configs) is in progress.
