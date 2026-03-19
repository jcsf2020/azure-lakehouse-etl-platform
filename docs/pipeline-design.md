# Pipeline Design

## 1. Pipeline Design Goals

- Ingest raw retail/eCommerce source data reliably into ADLS Gen2
- Apply structured transformations through Bronze → Silver → Gold layers
- Maintain clear separation between orchestration (ADF) and transformation (Databricks)
- Support incremental loads to avoid full re-ingestion on every run
- Provide auditable data quality checkpoints at layer boundaries
- Keep operational complexity low and failure recovery straightforward

---

## 2. High-Level Pipeline Sequence

[Source Systems: APIs + flat files]
              │
              ▼
[ADF: Land raw extracts in ADLS Gen2 / Bronze landing]
              │
              ▼
[ADF: Trigger Databricks Bronze standardisation job]
              │
              ▼
[Databricks: Bronze landing -> Bronze Delta]
              │
              ▼
[Databricks: Bronze -> Silver (cleanse, validate, deduplicate)]
              │
              ▼
[Databricks: Silver -> Gold (aggregate, model for analytics)]
              │
              ▼
[Gold Layer: Available for reporting / downstream consumers]

Pipeline runs are batch-oriented and scheduled daily. No streaming ingestion is in scope.

---

## 3. Pipeline Stages

| Stage | Layer | Owner | Description |
|---|---|---|---|
| Raw Landing | Bronze landing | ADF | Land raw API extracts and flat files as-is into ADLS Gen2 |
| Bronze Standardisation | Bronze Delta | Databricks | Convert landed raw data into queryable Bronze Delta tables with ingestion metadata |
| Cleanse & Validate | Silver | Databricks | Parse, type-cast, deduplicate, and conform entity datasets |
| Aggregate & Model | Gold | Databricks | Build facts, dimensions, and analytics-ready marts |

---

## 4. Pipeline Stages — Detail

### Bronze (Raw)

- Raw extracts from APIs and flat files are landed in ADLS Gen2 without business transformation.
- Files land in a partitioned folder structure such as `bronze/{entity}/year={}/month={}/day={}`.
- A Databricks standardisation step converts the landed raw data into Bronze Delta tables.
- Bronze Delta tables retain source fidelity and add ingestion metadata such as `_ingest_timestamp`, `source_system`, and `pipeline_run_id`.

### Silver (Cleansed)

- Databricks reads from Bronze Delta tables and applies:
  - Schema enforcement and type casting
  - Null handling and row-level validation
  - Deduplication based on primary key + ingestion date
  - Standardisation of string fields (trimming, casing)
- Output written to Delta Lake tables in the Silver container.
- Rejected rows are written to a quarantine path for review.

### Gold (Aggregated)

- Databricks reads from Silver and builds analytics-ready models:
  - Fact and dimension tables aligned to the retail domain (sales, products, customers, orders)
  - Pre-aggregated metrics (daily revenue, category performance, customer order frequency)
- Output written as Delta Lake tables in the Gold container.
- These tables are the primary consumption layer for reporting.

---

## 5. ADF Responsibilities

ADF acts as the orchestration layer. It does not perform data transformation.

- **Ingestion**: Copy Activity or API extraction activity lands raw source data in the Bronze landing zone in ADLS Gen2.
- **Trigger**: Once landing completes, ADF triggers Databricks jobs for Bronze standardisation and downstream transformation.
- **Scheduling**: Pipelines are triggered on a daily schedule using ADF triggers.
- **Control flow**: ADF manages conditional execution (e.g. skip transformation if no new files), retries, and failure alerting.
- **Parameterisation**: Pipeline parameters (date partition, source path, environment) are passed to downstream activities.

ADF does not read or write Delta Lake tables directly. All Delta operations are handled by Databricks.

---

## 6. Databricks Responsibilities

- **Landing -> Bronze Delta**: PySpark notebooks/jobs convert landed raw API/file extracts into Delta-backed Bronze tables.
- **Bronze → Silver**: PySpark notebooks/jobs that read raw files, apply schema, validate, and write to Silver Delta tables.
- **Silver → Gold**: PySpark jobs that aggregate and model Silver data into Gold Delta tables.
- **Delta Management**: MERGE (upsert) operations for incremental loads, schema evolution handling, and compaction where needed.
- **Data Quality**: Row-level validation logic is applied within the Spark jobs. Quarantine writes for failed rows.
- **Job parameters**: Jobs accept date parameters passed from ADF to control which partition to process.

Databricks does not manage scheduling or orchestration. It executes what ADF invokes.

---

## 7. Data Quality Checkpoints

Quality is enforced at two points in the pipeline:

**Bronze → Silver (row-level validation)**

- Required fields are not null
- Date fields parse correctly
- Numeric fields are within expected ranges
- Duplicate records are identified and removed
- Rows failing validation are written to `silver/quarantine/` with a failure reason column

**Silver → Gold (aggregate-level validation)**

- Row counts post-aggregation are compared against Silver source counts
- Key metrics (e.g. total revenue) are spot-checked for nulls or zeroes
- If aggregate validation fails, the Gold write is aborted and an alert is raised

No automated rollback is performed. Failed runs leave the previous valid Gold state intact.

---

## 8. Incremental Load Strategy

- **Landing (ADF)**: Copy Activity or API extraction uses a watermark pattern based on source last-modified date, source update timestamp, or pipeline run date. Only new or modified data is landed per run.
- **Bronze Delta -> Silver (Databricks)**: Jobs process the Bronze Delta partition matching the current run date. Delta MERGE is used to upsert records into Silver, using the record's primary key as the merge condition.
- **Silver → Gold (Databricks)**: Gold tables are rebuilt for the affected date partitions only. For aggregations that span rolling windows, only the affected partition range is recomputed.

Full reloads are supported by passing an explicit date range parameter, bypassing the watermark.

---

## 9. Failure Handling and Reprocessing

**ADF**

- Copy Activities are configured with a retry count of 2 with a 5-minute interval.
- Databricks job failures surface as ADF pipeline failures.
- Failed pipeline runs can be re-triggered manually from the ADF monitor with the same parameters.

**Databricks**

- Jobs are idempotent. Re-running a job for the same date partition produces the same result.
- Delta Lake's transaction log ensures partial writes do not corrupt existing table state.
- Quarantine tables retain failed rows so they can be inspected and reprocessed independently.

**Alerting**

- ADF pipeline failures trigger email alerts via ADF's built-in diagnostic settings.
- No custom alerting framework is in scope for this project.

---

## 10. Operational Notes

- All ADLS containers (bronze, silver, gold) use the same storage account with separate containers per layer.
- Databricks clusters are job clusters (not always-on). They are provisioned on-demand per run to minimise cost.
- Delta Lake tables use date-based partitioning for efficient incremental reads and writes.
- The pipeline processes one day of data per run. Backfills are performed by re-running the pipeline with a date range override.
- Source data used in this project is synthetic or anonymised for portfolio use. Formal PII handling controls are out of scope.

---

## 11. Scope Boundaries

The following are explicitly out of scope for this project:

- Streaming or near-real-time ingestion
- CI/CD pipeline automation (deployments are manual for this portfolio project)
- Data masking, encryption at the column level, or PII handling
- Full semantic layer design or production-grade BI delivery
- Multi-region or disaster recovery configuration
- SLA monitoring or formal data contracts between teams
- Unity Catalog or fine-grained Databricks access control

These boundaries reflect a deliberate decision to keep the project focused and demonstrable, not a gap in architectural awareness.
