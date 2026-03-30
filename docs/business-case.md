# Business Case: Azure Lakehouse ETL Platform

## Business Context

Retail and eCommerce companies generate transactional data across multiple operational systems: order management, product catalogues, customer records, and returns processing. As these businesses grow, this data fragments across systems with incompatible formats, varying update cadences, and no shared analytical layer.

Analysts work from stale exports, inconsistent spreadsheets, and ad hoc queries against operational databases not designed for analytical workloads. Decision-making slows. Confidence in reported numbers erodes. Revenue and return metrics diverge depending on which system you query.

This platform resolves that problem. It consolidates raw retail operational data into a single medallion lakehouse on Azure, delivering a SQL-queryable, DQ-validated analytical layer built on real executed assets — not a design sketch.

---

## Core Business Problem

A retail or eCommerce operation needs to answer time-sensitive questions about sales performance, return patterns, and channel mix — but cannot do so reliably because:

- Orders, customers, products, and returns are stored in separate systems with no unified access layer.
- There is no single source of truth for key metrics such as revenue per channel, return rate by product, or average order value.
- Joining data across sources (e.g. returns to original order lines) requires manual effort that does not scale and produces results that vary by analyst.
- Data freshness is unpredictable. The lag between an event occurring and being visible to analysts is measured in days.

The result: reporting is slow, contested, and disconnected from operational pace.

---

## Why Data Engineering Is Required

A BI tool alone does not solve this. The underlying data must be cleaned, joined, and modelled before it reaches analysts. Specifically:

- **Ingestion** from heterogeneous source formats (JSON orders, CSV returns, JSON customers and products) on a reliable daily schedule.
- **Standardisation** into a consistent schema and type system, filtering out malformed records before they reach analytical consumers.
- **Modelling** raw data into business-aligned entities: a line-level sales fact, a return fact with reconciliation status, dimension tables for customers and products, and pre-aggregated summaries by channel and return reason.
- **Data quality validation** that is explicit and queryable — not just logged — so stakeholders can verify the state of the data layer at any time.
- **Orchestration** that is reliable, retryable, and parameterised, so the pipeline can be re-run for any date without manual intervention.

Azure Data Factory, Azure Databricks, ADLS Gen2, Delta Lake, and Unity Catalog are the appropriate tools for this at production scale in the Azure ecosystem.

---

## Key Source Data

| Source | Format | Ingestion Method |
|---|---|---|
| eCommerce orders | JSON | ADF → Databricks Bronze notebook |
| Order line items | JSON | ADF → Databricks Bronze notebook |
| Customer records | JSON | ADF → Databricks Bronze notebook |
| Product catalogue | JSON | ADF → Databricks Bronze notebook |
| Returns events | CSV | ADF → Databricks Bronze notebook |

These five sources represent the minimum viable dataset for answering the core business questions below. They reflect common real-world integration challenges: nested JSON structures, schema drift captured via `_rescued_data`, and the need to reconcile return events against original order lines.

---

## Key Business Questions

The platform is designed to support the following analytical questions, each of which is answerable directly from Gold layer objects:

| Question | Gold Object |
|---|---|
| What is total revenue and order volume by sales channel? | `orders_channel_summary`, `fact_sales` |
| Which products have the highest return volume and refund cost? | `returns_by_product`, `fact_returns_enriched` |
| What are the most common return reasons, and what refund exposure do they carry? | `returns_reason_summary` |
| Which customers have returned items, and from which channel did they originally purchase? | `v_returns_enriched` |
| What is the line-level revenue breakdown by product, customer, and channel? | `v_sales_enriched` |
| Are there returns that cannot be matched to a sale record? | `v_dq_status` (`fact_returns_unmatched_sales` check) |
| What is the declared grain and type of every Gold analytical object? | `model_contract_v1` |

---

## Analytical Outputs — Delivered

The platform delivers the following Gold layer objects, all live in `lakehouse_prod.gold` under Unity Catalog:

**Fact tables**

| Table | Grain | Purpose |
|---|---|---|
| `fact_sales` | 1 row per order line | Line-level revenue analysis |
| `fact_returns_enriched` | 1 row per return event | Return reconciliation, refund analysis |

**Dimension tables**

| Table | Grain | Purpose |
|---|---|---|
| `dim_customers` | 1 row per customer | Customer filtering and segmentation |
| `dim_products` | 1 row per product | Product filtering, category and brand analysis |

**Aggregation tables**

| Table | Grain | Purpose |
|---|---|---|
| `orders_channel_summary` | 1 row per channel | Channel revenue and order count comparison |
| `returns_reason_summary` | 1 row per return reason | Return reason analysis, refund exposure |
| `returns_by_product` | 1 row per product | Product-level return volume and refund cost |

**Enrichment views**

| View | Purpose |
|---|---|
| `v_sales_enriched` | Self-service: sales with full customer and product context |
| `v_returns_enriched` | Self-service: returns with full customer and product context |

**Data quality and contract**

| Object | Purpose |
|---|---|
| `dq_summary_v1` | Queryable DQ check results (7 checks across dims and facts) |
| `v_dq_status` | PASS/FAIL status per check — primary post-run validation surface |
| `model_contract_v1` | Declared grain, object type, and layer for all Gold objects |

---

## Decision-Support Value

By delivering a reliable, reconciled, and DQ-validated analytical layer, the platform enables:

**Commercial and sales teams**
- Compare revenue and order volume by sales channel using `orders_channel_summary` or `v_sales_enriched`.
- Identify top and bottom performing products by line-level revenue from `fact_sales`.

**Operations and returns teams**
- Identify which products and return reasons drive the highest refund cost using `returns_by_product` and `returns_reason_summary`.
- Surface returns that cannot be reconciled to a sale record, quantified in `v_dq_status`.
- Analyse full return context (customer, product, channel, original order) via `v_returns_enriched`.

**Data and analytics teams**
- Validate the analytical layer state at any time via `v_dq_status`.
- Inspect declared grain for any Gold object via `model_contract_v1`.
- Rebuild any layer independently — all scripts are idempotent (`CREATE OR REPLACE`).

The platform reduces the time from data generation to analytical insight and removes dependency on manual data preparation across disconnected source systems.

---

## Scope Boundaries

This platform is scoped to the analytical layer. It does not:

- Replace or modify source operational systems.
- Provide streaming or near-real-time ingestion (batch-only).
- Implement SCD Type 2 historical versioning for customer or product dimensions.
- Model campaign attribution, paid media spend, or marketing analytics.
- Include financial consolidation or ERP reconciliation.
- Model inventory data (seed data exists; no SQL asset layer for inventory).
- Implement role-based access control beyond Unity Catalog registration.
- Include a production BI delivery layer (Power BI reports are not part of this platform).

The focus is a production-quality, end-to-end data engineering pipeline: reliable ingestion, structured transformation, SQL-driven serving, and an explicit data quality layer — fully executed on Azure.
