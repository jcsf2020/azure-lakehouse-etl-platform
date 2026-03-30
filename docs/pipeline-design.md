# Pipeline Design

## 1. Pipeline Design Goals

- Ingest raw retail source files reliably into ADLS Gen2 via Azure Data Factory
- Apply structured transformations through Bronze → Silver → Gold using Databricks
- Maintain strict separation between orchestration (ADF) and transformation (Databricks)
- Keep the Gold layer SQL-driven: all serving objects are defined as `CREATE OR REPLACE TABLE/VIEW`
- Support full rebuild on re-run without duplicating data (idempotent by design)
- Provide an explicit, queryable data quality layer at Gold

---

## 2. High-Level Pipeline Sequence

```
[Source files: CSV / JSON in ADLS Gen2]
              │
              ▼
[ADF: pl_orchestrate_lakehouse — master pipeline]
              │
              ▼
[ADF → Databricks: pl_bronze_ingestion]
  Databricks notebook reads seed files → writes to lakehouse_prod.bronze.*
              │
              ▼  (on success)
[ADF → Databricks: pl_silver_transformations]
  Databricks notebooks read bronze.* → write to lakehouse_prod.silver.*
  (parallel execution; order_items_clean waits for orders_clean + products_clean)
              │
              ▼  (on success)
[ADF → Databricks: pl_gold_aggregations]
  Databricks SQL job executes sql/gold/*, sql/dq/*, sql/contracts/*
  → writes to lakehouse_prod.gold.*
              │
              ▼
[Gold Layer: facts, dims, aggregates, views, DQ, contract available in Databricks SQL]
```

All runs are batch. No streaming component is in scope.

---

## 3. Pipeline Stages

| Stage | Layer | Executor | Description |
|---|---|---|---|
| Bronze ingestion | `lakehouse_prod.bronze` | Databricks (triggered by ADF) | Read seed files from ADLS Gen2, write as Delta tables |
| Silver transformation | `lakehouse_prod.silver` | Databricks (triggered by ADF) | Filter malformed records, write clean Delta tables |
| Gold build | `lakehouse_prod.gold` | Databricks SQL (triggered by ADF) | Execute all SQL DDL for facts, dims, aggregates, DQ, contract |

---

## 4. Pipeline Stages — Detail

### Bronze

- Databricks reads raw CSV and JSON seed files from ADLS Gen2 (`abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/`).
- Schema is inferred. `_rescued_data` captures fields that do not conform to the inferred schema.
- Five Delta tables are written to `lakehouse_prod.bronze`: `orders_raw`, `order_items_raw`, `customers_raw`, `products_raw`, `returns_raw`.
- `CREATE OR REPLACE TABLE` semantics — full rebuild on each run.

### Silver

- Databricks reads from `lakehouse_prod.bronze.*`.
- One notebook per entity; each applies `WHERE _rescued_data IS NULL` and drops the rescue column.
- Five Delta tables written to `lakehouse_prod.silver`: `orders_clean`, `order_items_clean`, `customers_clean`, `products_clean`, `returns_clean`.
- ADF activity dependency graph:
  - `silver_customers`, `silver_products`, `silver_orders`, `silver_returns` run in parallel.
  - `silver_order_items` depends on `silver_orders` (Succeeded) and `silver_products` (Succeeded).
- `CREATE OR REPLACE TABLE` semantics — full rebuild on each run.

### Gold

- A Databricks job executes the SQL DDL scripts in order:
  1. `sql/gold/01_fact_sales.sql`
  2. `sql/gold/02_dim_customers.sql`
  3. `sql/gold/03_dim_products.sql`
  4. `sql/gold/04_fact_returns_enriched.sql`
  5. `sql/gold/05_orders_channel_summary.sql`
  6. `sql/gold/06_returns_reason_summary.sql`
  7. `sql/gold/07_returns_by_product.sql`
  8. `sql/gold/08_v_sales_enriched.sql`
  9. `sql/gold/09_v_returns_enriched.sql`
  10. `sql/dq/01_dq_summary_v1.sql`
  11. `sql/dq/02_v_dq_status.sql`
  12. `sql/contracts/01_model_contract_v1.sql`
- All scripts use `CREATE OR REPLACE TABLE/VIEW` — fully idempotent, no state dependency from a prior run.
- `fact_returns_enriched` reads from `gold.fact_sales` — it must execute after `fact_sales`.
- `returns_by_product` reads from `gold.fact_returns_enriched` — it must execute after `fact_returns_enriched`.
- `dq_summary_v1` reads from multiple Gold tables — it must execute after all facts and dimensions.

---

## 5. ADF Responsibilities

ADF acts as the orchestration layer. It does not perform data transformation.

- **Scheduling**: `pl_orchestrate_lakehouse` is triggered on a daily schedule via an ADF trigger.
- **Sequencing**: Master pipeline executes three child pipelines in order: Bronze → Silver → Gold. Each stage uses `WaitOnCompletion: true`.
- **Parameterisation**: `pipeline_run_date` is passed through from master pipeline to all child pipelines and activity notebooks.
- **Triggering Databricks**: ADF uses the Databricks Notebook activity to trigger each Databricks notebook/job.
- **Failure handling**: Activity failures propagate to the parent pipeline. ADF Monitor surfaces errors with full activity-level logs.
- **Retry**: Copy Activities are configured with retry counts. Databricks activities surface as pipeline failures on first failure.

ADF does not read or write Delta Lake tables directly.

---

## 6. Databricks Responsibilities

- **Bronze notebooks**: Read raw files from ADLS Gen2, write Delta to `lakehouse_prod.bronze`.
- **Silver notebooks**: Read `lakehouse_prod.bronze.*`, filter and write to `lakehouse_prod.silver`.
- **Gold SQL execution**: Execute `CREATE OR REPLACE TABLE/VIEW` DDL from `sql/` directory against `lakehouse_prod.gold`.
- **Unity Catalog**: All writes go to the `lakehouse_prod` catalog, providing lineage and access control.
- **Data quality**: `dq_summary_v1` is executed as part of the Gold build. Results are immediately queryable via `v_dq_status`.

Databricks does not manage scheduling or triggers.

---

## 7. Data Quality

Data quality is enforced explicitly in the Gold layer via the DQ SQL assets.

**`gold.dq_summary_v1`** — built as part of the Gold execution stage. Runs seven checks:

| Check | Type |
|---|---|
| `dim_customers_duplicate_customer_id` | Uniqueness |
| `dim_customers_null_customer_id` | Completeness |
| `dim_products_duplicate_product_id` | Uniqueness |
| `dim_products_null_product_id` | Completeness |
| `fact_sales_duplicate_order_item_id` | Uniqueness |
| `fact_sales_null_order_item_id` | Completeness |
| `fact_returns_unmatched_sales` | Referential integrity |

**`gold.v_dq_status`** — derives PASS / FAIL per check from `dq_summary_v1`. Query this view after any Gold build to assess quality state.

Validated baseline: all checks PASS. `fact_returns_unmatched_sales = 3` is a known and expected count — `fact_returns_enriched` uses a LEFT JOIN that intentionally allows returns without a matching sale.

---

## 8. Idempotency and Reprocessing

- All Bronze, Silver, and Gold scripts use `CREATE OR REPLACE TABLE/VIEW`. Re-running any stage for any date produces the same deterministic result.
- There are no incremental watermarks or MERGE operations in the current SQL asset layer. Each run is a full rebuild.
- Re-running the full pipeline for a given date is safe. No duplicates are introduced.
- To rerun only the Gold stage, execute the Databricks Gold SQL job directly (bypassing ADF Bronze and Silver triggers). Silver data must already exist for the results to be correct.

---

## 9. Failure Handling

**ADF pipeline fails at Bronze**
- No Silver or Gold data changes.
- Investigate the Databricks notebook log from the ADF activity output.
- Fix the root cause and re-trigger `pl_orchestrate_lakehouse` with the same `pipeline_run_date`.

**ADF pipeline fails at Silver**
- Bronze is populated; Silver is partially populated or unchanged.
- Identify which Silver notebook failed via ADF Monitor activity logs.
- Inspect the Databricks job run logs.
- Fix and re-trigger the full pipeline, or trigger `pl_silver_transformations` directly.

**ADF pipeline fails at Gold**
- Silver is fully populated; Gold is unchanged from its last successful state.
- Review the Databricks job log for which SQL script failed.
- Fix the SQL asset and re-execute the Gold job from Databricks directly.
- The last successful Gold state remains intact until the next successful Gold build.

**Gold data incorrect but no failure raised**
- Query `v_dq_status` to surface any data quality violations.
- Inspect Silver tables for the issue date.
- Rebuild Gold by re-triggering the Gold execution job directly.

---

## 10. Operational Notes

- All ADLS containers (bronze, silver) use storage account `stazlakeetlweu01` in West Europe.
- Databricks clusters are job clusters — provisioned on demand per run, terminated on completion.
- The pipeline processes a full dataset per run. Partition-level incremental loads are not implemented.
- Backfills are executed by re-triggering the pipeline with the target `pipeline_run_date`. Each re-run overwrites existing data for that run.
- Do not trigger concurrent runs for the same date — `CREATE OR REPLACE` operations are not transactionally isolated across concurrent sessions.

---

## 11. Scope Boundaries

Out of scope:

- Streaming or near-real-time ingestion
- Delta MERGE / incremental upsert patterns
- Quarantine / rejection tables for malformed Silver records (rows are filtered out, not quarantined)
- CI/CD automated deployment of SQL assets or ADF pipeline definitions
- Unity Catalog fine-grained access policies
- Multi-region or disaster recovery patterns
- SLA monitoring or formal alerting beyond ADF email notifications
