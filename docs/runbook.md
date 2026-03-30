# Runbook

## 1. Operating Purpose

This runbook describes how to operate, monitor, and recover the Azure Lakehouse ETL Platform. It covers the standard daily run, manual re-runs, post-run validation, and response to common failure scenarios.

The platform is batch-oriented. It runs once per day and fully rebuilds Bronze, Silver, and Gold layers using `CREATE OR REPLACE TABLE/VIEW` semantics. There is no streaming component and no incremental watermark state to manage.

---

## 2. Standard Daily Run

The pipeline is triggered automatically by an ADF schedule trigger on `pl_orchestrate_lakehouse`.

**Execution sequence:**

1. ADF triggers `pl_bronze_ingestion` → Databricks reads seed files from ADLS Gen2 and writes to `lakehouse_prod.bronze.*`.
2. On success, ADF triggers `pl_silver_transformations` → Databricks notebooks write to `lakehouse_prod.silver.*` (parallel, with `order_items_clean` waiting on `orders_clean` and `products_clean`).
3. On success, ADF triggers `pl_gold_aggregations` → Databricks SQL job executes all Gold DDL assets in `sql/gold/`, `sql/dq/`, `sql/contracts/`.
4. Pipeline completes. All Gold tables and views are updated.

**Normal completion indicators:**

- All activities in ADF Monitor show `Succeeded`.
- `lakehouse_prod.gold.v_dq_status` — all checks return `PASS`. The check `fact_returns_unmatched_sales` may show a count of 3; this is expected and by design (LEFT JOIN semantics in `fact_returns_enriched`).
- `lakehouse_prod.gold.fact_sales` row count is consistent with `lakehouse_prod.silver.order_items_clean`.
- `lakehouse_prod.gold.dim_customers` row count matches `lakehouse_prod.silver.customers_clean`.

No manual intervention is required for a successful daily run.

---

## 3. Manual Re-run / Backfill Procedure

### Single date re-run

1. Open ADF Studio → Author → Pipelines.
2. Select `pl_orchestrate_lakehouse`.
3. Click **Add Trigger → Trigger Now**.
4. Set `pipeline_run_date` to the target date in `YYYY-MM-DD` format.
5. Confirm and submit.

All stages use `CREATE OR REPLACE`. Re-running for a date that has already been processed overwrites the existing data cleanly. No duplicates are introduced.

### Running a single stage independently

**Bronze only:**
Trigger the Databricks Bronze ingestion notebook directly from Databricks Jobs UI, passing `pipeline_run_date`.

**Silver only:**
Trigger `pl_silver_transformations` from ADF with the target date, or run individual Silver notebooks directly from Databricks. Bronze must be populated for correct results.

**Gold only:**
Trigger the Databricks Gold SQL job directly from Databricks Jobs UI. Silver must be fully populated for correct results. This is the most common targeted re-run scenario when fixing a Gold SQL asset.

### Backfill over a date range

The pipeline does not have a built-in range trigger. Execute single-date re-runs in sequence. For runs greater than a few days, trigger in small batches and monitor each batch before continuing.

Do not run concurrent pipeline executions for the same date — `CREATE OR REPLACE` is not transactionally isolated across concurrent sessions.

---

## 4. Validation Checks After a Run

After any run, perform these checks before marking the run complete.

### ADF Monitor

- Open ADF Studio → Monitor → Pipeline Runs.
- Confirm `pl_orchestrate_lakehouse` and all child pipelines show `Succeeded`.
- Review activity durations. Significant deviations from normal may indicate data volume changes or cluster sizing issues.

### Bronze validation

```sql
-- Verify Bronze tables are populated
SELECT 'orders_raw' AS tbl, COUNT(*) AS row_count FROM lakehouse_prod.bronze.orders_raw
UNION ALL SELECT 'order_items_raw', COUNT(*) FROM lakehouse_prod.bronze.order_items_raw
UNION ALL SELECT 'customers_raw', COUNT(*) FROM lakehouse_prod.bronze.customers_raw
UNION ALL SELECT 'products_raw', COUNT(*) FROM lakehouse_prod.bronze.products_raw
UNION ALL SELECT 'returns_raw', COUNT(*) FROM lakehouse_prod.bronze.returns_raw;
```

### Silver validation

```sql
-- Verify Silver tables are populated and consistent with Bronze
SELECT 'orders_clean' AS tbl, COUNT(*) AS row_count FROM lakehouse_prod.silver.orders_clean
UNION ALL SELECT 'order_items_clean', COUNT(*) FROM lakehouse_prod.silver.order_items_clean
UNION ALL SELECT 'customers_clean', COUNT(*) FROM lakehouse_prod.silver.customers_clean
UNION ALL SELECT 'products_clean', COUNT(*) FROM lakehouse_prod.silver.products_clean
UNION ALL SELECT 'returns_clean', COUNT(*) FROM lakehouse_prod.silver.returns_clean;
```

Row counts in Silver should be ≤ Bronze counts for each entity. A large gap may indicate elevated schema violation rates in the source data.

### Gold validation

```sql
-- Primary DQ check — run after every Gold build
SELECT check_name, issue_count, check_status
FROM lakehouse_prod.gold.v_dq_status
ORDER BY check_name;
```

Expected state: all rows show `check_status = 'PASS'`. The check `fact_returns_unmatched_sales` with `issue_count = 3` is expected and is a known design characteristic of the LEFT JOIN in `fact_returns_enriched`.

```sql
-- Spot-check core fact table
SELECT COUNT(*) AS total_lines, SUM(line_total) AS total_revenue
FROM lakehouse_prod.gold.fact_sales;

-- Spot-check return reconciliation
SELECT reconciliation_status, COUNT(*) AS cnt
FROM lakehouse_prod.gold.fact_returns_enriched
GROUP BY reconciliation_status;
```

---

## 5. Common Failure Scenarios

### ADF pipeline fails at Bronze

**Symptoms**: `pl_bronze_ingestion` activity fails. `lakehouse_prod.bronze.*` tables may be empty or from a prior run.

**Likely causes:**
- Source file not present or in an unexpected location in ADLS Gen2
- Databricks cluster startup failure
- ADLS Gen2 linked service credential issue

**Action:**
1. Check ADF Monitor for the specific activity error message.
2. Verify seed files exist at `abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/`.
3. Check Databricks job run logs for the Bronze notebook.
4. Resolve the root cause and re-trigger `pl_orchestrate_lakehouse`.

---

### ADF pipeline fails at Silver

**Symptoms**: Bronze succeeded; `pl_silver_transformations` fails. One or more Silver tables may be empty or stale.

**Likely causes:**
- Schema change in a Bronze table (unexpected column added or renamed)
- Databricks notebook runtime error
- `order_items_clean` dependency not satisfied (if `orders_clean` or `products_clean` failed)

**Action:**
1. Identify the failing activity in ADF Monitor.
2. Review the corresponding Databricks notebook log.
3. If `order_items_clean` failed due to a dependency: confirm `orders_clean` and `products_clean` completed successfully first.
4. Fix the root cause and re-trigger `pl_silver_transformations`, or trigger the full pipeline.

---

### ADF pipeline fails at Gold

**Symptoms**: Bronze and Silver succeeded; Gold tables are unchanged from the last successful run.

**Likely causes:**
- SQL syntax error in a Gold DDL script
- A Gold script reads from a Gold table that was not yet built (execution order violation)
- Databricks SQL job cluster startup failure

**Action:**
1. Review the Databricks Gold job run log for the specific SQL error.
2. Identify which script failed and which line.
3. Fix the SQL asset in `sql/gold/`, `sql/dq/`, or `sql/contracts/`.
4. Re-execute the Gold SQL job directly from Databricks Jobs UI (no need to re-run Bronze or Silver).
5. Re-run `v_dq_status` query to confirm Gold is healthy.

---

### `v_dq_status` shows unexpected FAIL

**Symptoms**: Pipeline succeeded but one or more DQ checks report `check_status = 'FAIL'`.

**Expected FAIL (by design):**
- `fact_returns_unmatched_sales` with `issue_count = 3` — this is intentional. `fact_returns_enriched` uses a LEFT JOIN; returns without a matching sale are captured, not discarded.

**Unexpected FAIL:**
- Duplicate primary keys in a dimension or fact table — investigate Silver for upstream duplicates.
- Null primary keys — investigate the Bronze source file for missing values.

**Action:**
1. Query `dq_summary_v1` to see the exact `issue_count`.
2. Query the affected Gold table to identify the offending records.
3. Trace back to Silver and then to Bronze to identify the root cause.
4. Fix the source data or the SQL transformation logic.
5. Re-run the Gold build and re-validate `v_dq_status`.

---

### Gold data looks incorrect but no pipeline failure

**Symptoms**: Pipeline succeeded and `v_dq_status` shows no unexpected failures, but a Gold metric looks wrong.

**Likely causes:**
- A filter condition in a Silver or Gold script is too aggressive, dropping valid records
- A JOIN condition in a Gold view is producing unexpected fan-out or record loss
- Source data changed in a way that is technically valid but semantically wrong

**Action:**
1. Query `v_dq_status` to rule out primary key and referential integrity issues.
2. Compare Bronze and Silver row counts to identify where records are being dropped.
3. Query the specific Gold table and trace the logic back through the SQL asset.
4. Fix the SQL asset and rebuild Gold.

---

## 6. Recovery and Reprocessing Notes

- **All scripts are idempotent.** `CREATE OR REPLACE TABLE/VIEW` ensures re-runs are safe at any layer.
- **Delta time travel is available.** Use `DESCRIBE HISTORY lakehouse_prod.gold.fact_sales` to inspect prior versions. Use `SELECT * FROM lakehouse_prod.gold.fact_sales VERSION AS OF <n>` to read a previous state.
- **The Gold layer holds its last successful state** until explicitly rebuilt. A failed Gold run does not overwrite existing Gold data — Delta Lake's transaction log ensures partial writes do not commit.
- **Silver data persists across Gold re-runs.** If only Gold needs to be rebuilt (e.g. after fixing a SQL asset), trigger the Gold job directly without re-running Bronze or Silver.
- **Bronze data persists across Silver re-runs.** If Silver needs to be rebuilt, trigger `pl_silver_transformations` or individual notebooks directly.

---

## 7. Logging and Monitoring

**ADF Monitor**
- Pipeline and activity run history: ADF Studio → Monitor → Pipeline Runs.
- Activity-level logs (input, output, error messages, duration) are accessible per activity.
- Failure alerts via Azure Monitor diagnostic settings on the ADF resource.

**Databricks Jobs UI**
- Job run history and per-run logs for all Bronze, Silver, and Gold notebook/job executions.
- Log retention follows Databricks workspace defaults.

**Data quality (Gold layer)**
- `lakehouse_prod.gold.v_dq_status` — query after every run for a PASS/FAIL summary.
- `lakehouse_prod.gold.dq_summary_v1` — query for exact issue counts per check.
- `lakehouse_prod.gold.model_contract_v1` — query to verify the declared grain of all Gold objects.

---

## 8. Operational Guardrails

- Do not manually delete ADLS Gen2 containers or Delta table directories. Use Unity Catalog DDL (`DROP TABLE`, `TRUNCATE TABLE`) if a table needs to be cleared.
- Do not modify a Gold SQL asset while a Gold job is executing. Wait for the job to complete or fail.
- Do not change ADF linked service credentials without testing in a non-production environment first.
- Treat `fact_returns_unmatched_sales = 3` as the expected baseline. Any increase in this count should be investigated as a potential data quality issue in the returns source.
- Gold tables should be considered stable once a daily run completes and `v_dq_status` confirms PASS. Do not re-run Gold for a date that downstream consumers have already read without communicating the change.

---

## 9. Scope Boundaries

Out of scope for this runbook:

- Infrastructure provisioning (ADLS Gen2, ADF, Databricks workspace setup)
- Databricks cluster sizing and autoscaling configuration
- Unity Catalog access control and permission management
- ADF linked service and integration runtime setup
- Source system integration details (source files are assumed to be present in ADLS Gen2 before pipeline run)
- Data retention and archive policies
- Streaming or near-real-time pipeline operations
