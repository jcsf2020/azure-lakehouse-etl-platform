# Runbook

## 1. Operating Purpose

This runbook describes how to operate, monitor, and recover the Azure Lakehouse ETL Platform in practice. It covers the standard daily run, how to trigger manual re-runs or backfills, how to validate output, and how to respond to common failures.

The platform is batch-oriented. It runs once per day and processes the previous day's data through Bronze → Silver → Gold. There is no streaming component.

---

## 2. Standard Daily Run

The pipeline is triggered automatically by an ADF schedule trigger configured to run at a fixed time each day (e.g. 02:00 UTC).

**What happens automatically:**

1. ADF Copy Activity ingests raw source files into the Bronze ADLS container, partitioned by date.
2. ADF triggers the Databricks job for Bronze → Silver processing, passing the run date as a parameter.
3. On success, ADF triggers the Databricks job for Silver → Gold processing.
4. ADF pipeline completes. Status is visible in the ADF Monitor.

**Normal completion indicators:**
- All ADF pipeline activities show status `Succeeded` in the ADF Monitor.
- New partitions exist in the Silver and Gold Delta tables for the run date.
- No new rows appear in the Silver quarantine path for unexpected entities.

No manual intervention is required for a successful daily run.

---

## 3. Manual Re-run / Backfill Procedure

Use this procedure when a run needs to be repeated for a specific date, or when catching up on missed dates.

### Single date re-run

1. Open ADF Studio → Author → Pipelines.
2. Select the main pipeline.
3. Click **Add Trigger → Trigger Now**.
4. In the parameter prompt, set `run_date` to the target date in `YYYY-MM-DD` format.
5. Confirm and submit.

The pipeline is idempotent. Re-running for a date that has already been processed will overwrite Bronze files and upsert Silver/Gold Delta partitions. No duplicates will be introduced.

### Backfill over a date range

There is no built-in range trigger. Backfills are executed by triggering the pipeline once per date in sequence. For a large backfill (more than a few days), trigger runs in batches and monitor each before continuing.

> Do not trigger a high volume of concurrent runs. Databricks job clusters are provisioned per run; running many in parallel will increase cost and may hit subscription limits.

### Running a single stage independently

If only the Silver → Gold stage needs to be re-run (e.g. after a logic fix), the Databricks job can be triggered directly from the Databricks Jobs UI, passing the `run_date` parameter manually. This bypasses ADF ingestion and Bronze → Silver processing.

---

## 4. Validation Checks After a Run

After any run (scheduled or manual), perform the following checks before considering the run complete.

**ADF Monitor**
- All pipeline activities show `Succeeded`.
- Duration is within the normal range. A significantly longer run may indicate a cluster sizing issue or data volume spike.

**Bronze layer**
- Confirm new files exist at `bronze/{entity}/year={y}/month={m}/day={d}/` for each expected source entity.
- File count and approximate size should be consistent with prior days.

**Silver layer**
- Query the Silver Delta table and filter by `_ingest_timestamp` for the run date. Row counts should be consistent with the Bronze source.
- Check the quarantine path `silver/quarantine/` for any new files written during the run. Investigate any unexpected quarantine volume.

**Gold layer**
- Spot-check key aggregate metrics for the run date (e.g. total order count, total revenue).
- Compare against the prior day's values. Large deviations warrant investigation before downstream consumers use the data.

---

## 5. Common Failure Scenarios

### ADF Copy Activity fails

**Symptoms**: ADF pipeline fails at the ingestion step. No new Bronze files.

**Likely causes**:
- Source system unavailable or returned an unexpected format
- ADLS Gen2 connectivity issue
- Incorrect linked service credentials

**Action**:
1. Check the ADF activity error message in the Monitor.
2. Verify the source is reachable and the file format has not changed.
3. Confirm the ADLS linked service credentials are valid.
4. Re-trigger the pipeline after resolving the root cause.

---

### Databricks job fails — Bronze → Silver

**Symptoms**: ADF pipeline fails at the first Databricks activity. Silver table has no new partition for the run date.

**Likely causes**:
- Schema change in a source file not handled by the current job
- Null or malformed values causing a parsing error
- Cluster startup failure (rare)

**Action**:
1. Open the Databricks Jobs UI and review the failed run's log output.
2. Identify the specific error (parse failure, schema mismatch, etc.).
3. If the source data is malformed, check whether the quarantine path caught rows or whether the job aborted entirely.
4. Fix the underlying issue (schema update, data correction) and re-run the pipeline for the affected date.

---

### Databricks job fails — Silver → Gold

**Symptoms**: ADF pipeline fails at the second Databricks activity. Silver table updated, Gold table has no new partition.

**Likely causes**:
- Aggregate validation check triggered an abort (e.g. null metric values in Silver)
- Logic error in a Gold transformation (e.g. division by zero, unexpected join result)

**Action**:
1. Review the Databricks job log for the specific failure message.
2. If the abort was triggered by the validation check, inspect the Silver data for the run date to confirm the issue.
3. Fix the logic or data issue and re-run the Silver → Gold job directly from the Databricks Jobs UI.

---

### Quarantine volume is unexpectedly high

**Symptoms**: Run completes but quarantine path contains a large number of rejected rows.

**Likely causes**:
- Source data quality degraded (new nulls, changed field formats)
- A new upstream feed format was deployed without notice

**Action**:
1. Query the quarantine table and inspect the `failure_reason` column.
2. Determine whether the issue is systematic (all rows from one entity) or isolated.
3. If systematic, escalate to the source data owner. Do not promote quarantined data to Silver without review.

---

### Gold data looks incorrect but no failure was raised

**Symptoms**: Pipeline succeeded but a Gold metric looks wrong (e.g. revenue is zero, order count is implausibly low).

**Likely causes**:
- A silent data quality issue in Silver that passed row-level validation but produced bad aggregates
- A join condition in the Gold job dropped rows unexpectedly

**Action**:
1. Query Silver for the run date and verify the raw data looks correct.
2. Re-run the Silver → Gold job after identifying and fixing the logic issue.
3. Consider tightening the aggregate validation thresholds to catch this class of issue in future runs.

---

## 6. Recovery / Reprocessing Notes

- **All Databricks jobs are idempotent.** Re-running a job for the same `run_date` is safe. Delta MERGE ensures no duplicate records are introduced in Silver. Gold partitions for the date are overwritten cleanly.
- **Bronze files are not deleted on re-run.** ADF Copy Activity will overwrite the existing Bronze files for the same partition.
- **Delta time travel is available** on Silver and Gold tables. If a bad write needs to be inspected, use `DESCRIBE HISTORY` and `VERSION AS OF` to examine the table state before the bad run. Restoring a previous version is possible but should be done with care to avoid downstream inconsistency.
- **The Gold layer holds the last known good state** until explicitly overwritten. A failed Silver → Gold run does not corrupt the existing Gold data.

---

## 7. Logging and Monitoring Notes

**ADF Monitor**
- All pipeline runs are visible in ADF Studio under Monitor → Pipeline Runs.
- Activity-level logs (input/output, duration, error messages) are accessible per run.
- ADF is configured to send failure alerts via email using Azure Monitor diagnostic settings.

**Databricks**
- Job run history is available in the Databricks Jobs UI under each job's run history tab.
- Stdout and stderr logs are available per cluster run and are retained for the default Databricks log retention period.
- No external log aggregation (e.g. Log Analytics, Splunk) is configured in this project.

**ADLS Gen2**
- Storage diagnostic logs can be enabled in the Azure portal if access-level troubleshooting is needed.
- Not enabled by default in this project.

**What is not in scope:**
- Centralised alerting beyond ADF email notifications
- Custom dashboards or uptime monitoring
- On-call rotation or incident management tooling

---

## 8. Operational Guardrails

- Do not manually delete Bronze, Silver, or Gold ADLS containers or Delta table directories. Use targeted partition removal if a specific date needs to be cleared.
- Do not modify live Databricks job definitions during a running pipeline. Wait for the current run to complete or fail before making changes.
- Do not change ADF linked service credentials without verifying the new credentials work end-to-end in a non-production environment first.
- Treat the quarantine path as read-only from an operational standpoint. Do not delete quarantine files without first understanding why they were rejected.
- Gold tables should be considered stable once a daily run completes successfully. Avoid re-running Gold for a date that downstream consumers have already read without communicating the change.

---

## 9. Scope Boundaries

The following are out of scope for this runbook:

- Infrastructure provisioning (Terraform, ARM templates, or manual Azure portal setup are covered separately)
- Databricks cluster configuration or autoscaling tuning
- ADLS Gen2 access control and RBAC management
- Source system integration details (source connectivity is abstracted via ADF linked services)
- Data retention and archival policies
- Any streaming or near-real-time pipeline operations

This runbook covers day-to-day batch pipeline operations only.
