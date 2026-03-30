# Architecture

## Architecture Overview

This platform implements a **medallion lakehouse architecture** on Azure for retail and eCommerce analytics. Data flows from operational source files through three structured layers — Bronze, Silver, and Gold — before being served via Databricks SQL under Unity Catalog.

The design maintains a strict separation between orchestration (Azure Data Factory), transformation compute (Azure Databricks), and storage (ADLS Gen2). The Gold layer is SQL-driven: all serving objects are defined as Delta tables and views executed in Databricks SQL, not Python aggregations.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Data Sources                                   │
│            (JSON / CSV flat files — orders, products, customers,        │
│             order items, returns)                                        │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                      Azure Data Factory (ADF)                            │
│          Orchestration · Scheduling · Pipeline Control Flow              │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          ADLS Gen2  ·  stazlakeetlweu01  (West Europe)                   │
│  ┌────────────────┐  ┌────────────────┐                                  │
│  │  Bronze zone   │─▶│  Silver zone   │                                  │
│  │  (raw Delta)   │  │  (clean Delta) │                                  │
│  └────────────────┘  └────────────────┘                                  │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Azure Databricks · Unity Catalog                      │
│               lakehouse_prod.bronze / .silver / .gold                    │
│                                                                          │
│  Bronze → Silver: PySpark notebooks                                      │
│  Silver → Gold:  SQL DDL executed in Databricks                          │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                Gold Layer  ·  Databricks SQL                             │
│   Facts · Dimensions · Aggregates · DQ Table/View · Model Contract       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

| Component | Role |
|---|---|
| **Azure Data Factory (ADF)** | Orchestrates and schedules all pipeline stages. Triggers Databricks notebooks/jobs for Bronze → Silver and Gold execution. Does not perform data transformation. |
| **ADLS Gen2 (`stazlakeetlweu01`)** | Unified storage for all medallion zones. Backs all Delta tables across Bronze, Silver, and Gold layers. |
| **Delta Lake** | Table format providing ACID transactions, schema enforcement, and time travel. Used at all three layers. |
| **Azure Databricks** | Compute engine. PySpark notebooks execute Bronze and Silver processing. Databricks SQL executes all Gold DDL assets. |
| **Unity Catalog** | Governs the `lakehouse_prod` catalog namespace. All Bronze, Silver, and Gold objects are registered under `lakehouse_prod.{layer}.{table}`. |

---

## End-to-End Data Flow

1. **Bronze ingestion** — ADF triggers the Databricks Bronze ingestion notebook. Raw CSV and JSON seed files are read from ADLS Gen2 and written as Delta tables in `lakehouse_prod.bronze`. Schema is inferred; `_rescued_data` captures malformed fields.

2. **Bronze → Silver** — ADF triggers Silver transformation notebooks in Databricks (in parallel where possible, with dependency ordering for `order_items_clean`). Each Silver notebook filters out rows where `_rescued_data IS NOT NULL` and drops the rescue column, producing clean, typed Delta tables in `lakehouse_prod.silver`.

3. **Silver → Gold** — A Databricks SQL job executes the Gold DDL assets defined in `sql/gold/`, `sql/dq/`, and `sql/contracts/`. Each script runs `CREATE OR REPLACE TABLE/VIEW`, making Gold builds fully idempotent. Gold reads exclusively from Silver (facts join Silver tables; dimensions are sourced directly from Silver).

4. **Serving** — All Gold objects are queryable via Databricks SQL under `lakehouse_prod.gold`. The `v_dq_status` view provides a real-time data quality summary. The `model_contract_v1` table declares the grain and type of every Gold object.

---

## Medallion Layer Responsibilities

### Bronze — Raw Zone (`lakehouse_prod.bronze`)

- Stores data exactly as received from source files.
- Schema inference via Databricks `_rescued_data` mechanism — malformed fields are captured, not lost.
- No business transformations applied.
- Five tables: `orders_raw`, `order_items_raw`, `customers_raw`, `products_raw`, `returns_raw`.
- Source: `abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/`.

### Silver — Cleansed Zone (`lakehouse_prod.silver`)

- Filters out records with schema violations (`_rescued_data IS NOT NULL`).
- Drops the `_rescued_data` column for downstream cleanliness.
- Preserves natural keys; no surrogate key generation.
- Five tables mirror the five Bronze tables with a `_clean` suffix.
- Grain is enforced: one row per natural key per entity.

### Gold — Business Zone (`lakehouse_prod.gold`)

- All objects are SQL-defined (`CREATE OR REPLACE TABLE/VIEW`). No Python transformation at this layer.
- Two fact tables: `fact_sales` (line-level revenue) and `fact_returns_enriched` (return events with LEFT JOIN reconciliation).
- Two dimension tables: `dim_customers` and `dim_products` (Type 1 overwrite; natural keys, no surrogate keys).
- Three aggregation tables: `orders_channel_summary`, `returns_reason_summary`, `returns_by_product`.
- Two enrichment views: `v_sales_enriched`, `v_returns_enriched`.
- One data quality table (`dq_summary_v1`) and one DQ view (`v_dq_status`).
- One model contract table (`model_contract_v1`) declaring grain and layer for all Gold objects.

---

## Orchestration vs Transformation Responsibilities

**Azure Data Factory is responsible for:**
- Scheduling pipeline runs (daily trigger).
- Sequencing pipeline stages: Bronze → Silver → Gold via `pl_orchestrate_lakehouse`.
- Triggering Databricks notebooks and jobs via the ADF Databricks Notebook activity.
- Passing the `pipeline_run_date` parameter to all child pipelines.
- Monitoring pipeline success or failure and surfacing alerts.
- Retry logic at the pipeline level.

**Azure Databricks is responsible for:**
- Executing PySpark transformation logic for Bronze and Silver layers.
- Executing SQL DDL for all Gold layer objects.
- Writing output to Delta tables in ADLS Gen2 under Unity Catalog.
- Data quality checks (via `dq_summary_v1` execution).

ADF does not contain transformation logic. Databricks does not manage scheduling. This boundary is enforced in the pipeline definitions.

---

## Serving Layer

The Gold layer is the authoritative analytical surface. Two serving patterns are in use:

**Databricks SQL (primary)**
- All `lakehouse_prod.gold.*` tables and views are queryable directly.
- `v_sales_enriched` and `v_returns_enriched` provide pre-joined, self-service views combining facts with customer and product context.
- `v_dq_status` provides a live quality check summary.

**Power BI (optional)**
- Connects to Gold Delta tables or a Databricks SQL endpoint.
- Retail KPIs: sales performance, return analysis, channel comparison.

---

## Operational Considerations

**Idempotency** — All Gold DDL uses `CREATE OR REPLACE TABLE/VIEW`. Re-running any Gold script for any date produces the same deterministic result.

**Data quality** — `dq_summary_v1` runs seven declarative checks: primary key uniqueness and null checks for dimensions and `fact_sales`, plus a referential integrity check for unmatched returns. Results are surfaced via `v_dq_status` (PASS/FAIL per check). Validated state: all PASS except `fact_returns_unmatched_sales = 3`, which is expected — the LEFT JOIN in `fact_returns_enriched` intentionally captures returns without a matched sale.

**Intentional LEFT JOIN semantics** — `fact_returns_enriched` joins `returns_clean` to `fact_sales` via LEFT JOIN on `order_item_id`. Returns with no matching sale record receive `reconciliation_status = 'UNMATCHED'` and retain null join columns. This is by design; unmatched returns are surfaced, not discarded.

**Delta Lake** — ACID transactions protect against partial writes. Time travel is available on all layers for audit and reprocessing.

---

## Scope Boundaries

Intentionally out of scope:

- **Streaming ingestion** — All flows are batch. No Event Hubs or Structured Streaming.
- **SCD Type 2** — Dimension tables are Type 1 (overwrite). Historical versioning is not implemented in the current SQL assets.
- **Surrogate keys** — Natural keys are used throughout. No SHA-256 or sequence-generated surrogate keys.
- **Infrastructure as code** — No Terraform or Bicep. Resources are provisioned via the Azure portal.
- **CI/CD for SQL assets** — SQL execution is manual or Databricks job-triggered; no automated deployment pipeline for DDL.
- **Multi-region / disaster recovery** — Single Azure region deployment.
