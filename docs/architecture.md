# Architecture

## Architecture Overview

This platform implements a **medallion lakehouse architecture** on Azure, designed for retail and eCommerce analytics. Data flows from operational sources through three structured layers — Bronze, Silver, and Gold — before being served to analytical consumers.

The design prioritises clarity of responsibility, reproducibility, and maintainability over complexity. Each component has a well-defined role, and the boundaries between orchestration, transformation, and serving are intentionally explicit.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Data Sources                                   │
│          (Flat files, REST APIs, Operational DBs, SaaS exports)         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                      Azure Data Factory (ADF)                            │
│               Ingestion · Scheduling · Pipeline Orchestration            │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   ADLS Gen2  ·  Delta Lake Storage                       │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐ │
│  │  Bronze Layer  │  │  Silver Layer  │  │       Gold Layer           │ │
│  │  (raw zone)    │─▶│  (clean zone)  │─▶│  (business / serving zone) │ │
│  └────────────────┘  └────────────────┘  └────────────────────────────┘ │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Azure Databricks  ·  PySpark                          │
│              Cleansing · Enrichment · Aggregation · dbt (optional)       │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         Serving Layer                                    │
│           Power BI  ·  Databricks SQL  ·  External consumers             │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

| Component | Role |
|---|---|
| **Azure Data Factory (ADF)** | Ingestion pipelines and pipeline orchestration. Responsible for moving raw data into the Bronze layer on a schedule or trigger. |
| **ADLS Gen2** | Unified storage layer for all medallion zones. Acts as the data lake backend for Delta tables. |
| **Delta Lake** | Table format providing ACID transactions, schema enforcement, and time travel across all three medallion layers. |
| **Azure Databricks** | Compute engine for all transformation workloads. PySpark notebooks and jobs handle cleansing, enrichment, and aggregation across Silver and Gold layers. |
| **dbt (optional)** | SQL-based transformation layer targeting Databricks SQL. Used to model Gold layer assets with lineage and documentation built in. |
| **Power BI (optional)** | Primary BI and reporting surface. Connects directly to Gold layer Delta tables or Databricks SQL endpoints. |

---

## End-to-End Data Flow

1. **Ingestion** — ADF pipelines pull data from source systems (CSV files, REST APIs, database exports) on a defined schedule. Raw files land in the Bronze zone of ADLS Gen2 with minimal transformation.

2. **Bronze → Silver** — A Databricks job reads the raw Delta tables in Bronze, applies schema validation, deduplication, and light cleansing, and writes the result to Silver as clean, typed Delta tables.

3. **Silver → Gold** — A second Databricks job (or dbt model) aggregates and joins Silver tables into business-oriented entities — fact tables, dimension tables, and summary datasets — and writes them to Gold.

4. **Serving** — Power BI reports and dashboards connect to Gold layer tables. Ad-hoc queries run against Databricks SQL. External consumers can query Gold tables directly via the Delta Sharing protocol or SQL endpoints.

All layers are append-or-overwrite Delta tables. Partitioning strategies are applied at the Silver and Gold layers based on query access patterns (typically by date or category).

---

## Medallion Layer Responsibilities

### Bronze — Raw Zone

- Stores data exactly as received from source systems.
- No schema enforcement beyond what the file format provides.
- Retains full history; nothing is deleted.
- Serves as the audit and reprocessing baseline.
- Format: Delta Lake (converted from raw CSV/JSON/Parquet on landing).

### Silver — Cleansed Zone

- Applies schema enforcement, null handling, and type casting.
- Deduplicates records using business keys.
- Standardises date formats, currency, and categorical values.
- Joins across sources are minimal here; this layer stays close to source entities.
- Format: Delta Lake with enforced schema and partitioning.

### Gold — Business Zone

- Aggregates and joins Silver tables into analytical models.
- Implements business logic: revenue calculations, customer segmentation, inventory metrics.
- Structures data into a dimensional model (star schema) suitable for BI tooling.
- Optimised for read performance: Z-ordering, liquid clustering (where applicable), and materialised aggregates.
- Format: Delta Lake, optionally exposed via Databricks SQL or Delta Sharing.

---

## Orchestration vs Transformation Responsibilities

A common source of confusion in lakehouse projects is where orchestration ends and transformation begins. This platform keeps these responsibilities cleanly separated.

**Azure Data Factory is responsible for:**
- Triggering and scheduling pipeline runs.
- Moving data from external sources into Bronze storage.
- Monitoring pipeline success or failure.
- Calling Databricks jobs via the ADF Databricks Notebook or Job activity.
- Handling retry logic and alerting at the pipeline level.

**Azure Databricks is responsible for:**
- All data transformation logic (PySpark or SQL).
- Writing output to Silver and Gold Delta tables.
- Schema evolution and data quality checks within the transformation code.
- Running dbt models if applicable.

ADF does not contain transformation logic. Databricks does not manage scheduling or ingestion triggers. This separation keeps each layer testable and independently maintainable.

---

## Serving Layer

The Gold layer is the authoritative source for analytical consumption. Two serving patterns are supported:

**Power BI (interactive reporting)**
- Connects to Gold Delta tables via the Databricks connector or a SQL endpoint.
- Reports cover retail KPIs: sales performance, basket analysis, inventory turnover, customer cohort metrics.
- Datasets are imported or run in DirectQuery depending on data volume and refresh requirements.

**Databricks SQL (ad-hoc and self-service)**
- Analysts query Gold tables directly using Databricks SQL warehouses.
- Supports exploratory analysis and one-off business questions without requiring a BI tool.

**External consumers (optional)**
- Gold tables can be shared externally via Delta Sharing.
- REST-based consumers can query aggregated data through a lightweight API layer if required (not included in the core scope of this project).

---

## Operational Considerations

**Idempotency** — All transformation jobs are written to be idempotent. Re-running a job for the same date partition produces the same result without duplicating data.

**Incremental loads** — Where possible, Silver and Gold jobs process only new or changed data using Delta Lake's `MERGE` operation or partition pruning on ingestion date.

**Data quality** — Basic expectation checks (row counts, null rates, referential integrity) are implemented as assertions within Databricks jobs. Failures halt the pipeline and surface in ADF monitoring.

**Logging and observability** — Job run logs are written to a dedicated Delta table in the Silver layer. ADF pipeline run history provides a secondary audit trail.

**Cost control** — Databricks clusters use autoscaling with a defined minimum and maximum node count. Jobs use spot instances where tolerated. Clusters are configured to terminate after a period of inactivity.

**Schema evolution** — Delta Lake's schema evolution features (`mergeSchema`) are used conservatively. Breaking schema changes require a manual migration step to prevent silent downstream issues.

---

## Scope Boundaries

The following are intentionally out of scope for this project:

- **Streaming ingestion** — All data flows are batch. Near-real-time patterns (Structured Streaming, Event Hubs) are not included.
- **Infrastructure as code** — Terraform or Bicep templates are not part of this project. Resources are assumed to be provisioned manually or through the Azure portal.
- **Network and security architecture** — Private endpoints, VNet injection, and managed identity configuration are referenced but not detailed. These are environment-specific concerns.
- **Data governance** — Unity Catalog lineage, access policies, and data classification are noted as production best practices but are not fully implemented here.
- **Multi-region or disaster recovery** — The platform targets a single Azure region. HA and DR patterns are not in scope.
- **CI/CD pipelines** — Automated deployment of ADF pipelines and Databricks notebooks is not included, though the project structure supports it.

These boundaries are deliberate. The goal is a clear, functional, and explainable platform — not a reference architecture for an enterprise of thousands.
