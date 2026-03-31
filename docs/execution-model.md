# Execution Model

## Overview

This platform has two coherent execution paths. Both operate on the same SQL assets and produce identical objects under `lakehouse_prod.*`. They differ in how Bronze → Silver is executed and what drives orchestration.

| | SQL-first runtime | ADF + Notebooks |
|---|---|---|
| **Status** | Validated against Databricks SQL Warehouse | Production deployment target |
| **Entry point** | `scripts/run_full_pipeline.py` | ADF `pl_orchestrate_lakehouse` |
| **Bronze → Silver** | `read_files()` SQL + CTAS in `sql/bronze/`, `sql/silver/` | PySpark notebooks in `databricks/notebooks/` |
| **Gold → DQ → Contracts** | SQL assets in `sql/gold/`, `sql/dq/`, `sql/contracts/` | Same SQL assets |
| **Compute** | Databricks SQL Warehouse (all layers) | Databricks cluster (Bronze/Silver) + SQL Warehouse (Gold) |
| **Orchestration** | Python harness | Azure Data Factory |

---

## Path 1: SQL-first runtime (validated)

### What it does

`scripts/run_full_pipeline.py` is a Python harness that:

1. Discovers SQL assets in `sql/bronze/`, `sql/silver/`, `sql/gold/`, `sql/dq/`, `sql/contracts/`
2. Opens a connection to a Databricks SQL Warehouse (via `databricks-sql-connector`)
3. Executes each file in dependency order — within each layer, sorted by filename prefix (`01_`, `02_`, ...)
4. Validates all DQ results from `lakehouse_prod.gold.v_dq_status`
5. Exports artifact CSV files and a structured `run_log.json` to `artifacts/execution_runs/<run_id>/`

### Invocation

```bash
export DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
export DATABRICKS_TOKEN=<pat-or-service-principal-secret>
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx

python scripts/run_full_pipeline.py               # Full run — all layers
python scripts/run_full_pipeline.py --skip-bronze # Silver onward — Bronze already populated
python scripts/run_full_pipeline.py --dry-run     # Validate asset discovery; no execution
```

Exit codes: `0` for SUCCESS or DRY_RUN, `1` for FAILED or DQ_WARNING.

### Bronze execution in this path

The Bronze SQL scripts (`sql/bronze/*.sql`) use the `read_files()` table-valued function:

```sql
CREATE OR REPLACE TABLE lakehouse_prod.bronze.returns_raw USING DELTA AS
SELECT
  CAST(return_id AS STRING) AS return_id,
  ...
  _rescued_data
FROM read_files(
  'abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/returns.csv',
  format => 'csv', header => true, inferSchema => true
);
```

This is complete SQL-only ingestion: no PySpark, no Spark cluster. The SQL Warehouse reads directly from ADLS Gen2 and writes a typed Delta table with `_rescued_data` for malformed fields.

### Execution evidence

Run `20260330_222422` (SUCCESS, 47 seconds):

- Steps executed: silver (5) + gold (9) + dq (2) + contracts (1) = 17 SQL assets
- DQ results: 7 checks — 6 PASS, 1 expected FAIL (`fact_returns_unmatched_sales = 3`, by design)
- Artifacts committed: `artifacts/execution_runs/20260330_222422/`

The run used `--skip-bronze` — Bronze tables were already populated from a prior full run.

---

## Path 2: ADF + Notebooks (production deployment target)

### What it does

Azure Data Factory orchestrates the pipeline via `pl_orchestrate_lakehouse`:

1. `pl_bronze_ingestion` → triggers the Databricks Bronze ingestion notebook, which reads seed files from ADLS Gen2 using PySpark and writes Delta tables to `lakehouse_prod.bronze.*`
2. `pl_silver_transformations` → triggers Silver transformation notebooks (parallel execution with dependency ordering for `order_items_clean`); PySpark filters `_rescued_data IS NOT NULL` and writes to `lakehouse_prod.silver.*`
3. `pl_gold_aggregations` → triggers a Databricks SQL job that executes `sql/gold/`, `sql/dq/`, `sql/contracts/`

### What implements this path

| Asset | Role |
|---|---|
| `src/azure_lakehouse_etl/` | Python transformation library — Bronze → Silver PySpark logic |
| `databricks/notebooks/` | Thin notebook entrypoints that import and call the library |
| `adf/pipelines/` | ADF pipeline definitions (Bronze, Silver, Gold, master) |
| `sql/gold/`, `sql/dq/`, `sql/contracts/` | Gold layer SQL assets — shared with Path 1 |

The Python library is smoke-tested locally via `tests/quality/`. These tests validate import and module structure; they do not execute live Databricks code.

---

## Hardcodes

Two hardcodes affect multi-environment deployability. Both are acknowledged. Neither is a blocking issue for the current single-environment scope.

### Catalog name: `lakehouse_prod`

Hardcoded in every SQL file across all layers and in the artifact export queries in `run_full_pipeline.py`.

`databricks.yml` defines `catalog` variables (`dev_catalog`, `prod_catalog`) per target, but these variables are **not currently wired to SQL execution**. The comment in `databricks.yml` is explicit:

> "This variable is not wired to SQL asset execution — all SQL files currently hardcode the catalog name directly."

**Impact**: Targeting a different catalog requires find-and-replace across all SQL files. This blocks automated multi-environment promotion until a parameterisation pass is completed.

### Storage account: `stazlakeetlweu01`

Hardcoded in all Bronze SQL scripts inside the `read_files()` path:

```
abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/
```

**Impact**: Affects SQL-first Bronze execution only. Changing the storage account (e.g. for a dev environment with a separate storage account) requires updating all Bronze SQL scripts.

---

## Promotion path

The steps required to move from the current validated state to a fully automated Databricks Workflow deployment:

1. **Parameterise catalog references** — wire `databricks.yml` `catalog` variables to SQL execution so `dev` and `prod` targets resolve to different catalogs without manual find-and-replace
2. **Package SQL assets** — resolve SQL files from a packaged artifact path rather than the local filesystem, so `run_full_pipeline.py` or a Gold SQL job can execute inside a Databricks Workflow
3. **Define a Databricks Workflow** — add a `resources.jobs` block to `databricks.yml` for scheduled execution (replacing or complementing ADF for the SQL-first path)
4. **Add workspace credentials to CI** — enable `databricks bundle deploy` in GitHub Actions after secrets are provisioned
5. **Define a Databricks secret scope** — manage `DATABRICKS_TOKEN` and connection config via a secret scope rather than environment variables

None of these steps require changes to the SQL business logic or the existing Python library.
