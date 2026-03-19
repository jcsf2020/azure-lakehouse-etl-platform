# Data Model

## 1. Modelling Approach

This platform follows a **medallion architecture** (Bronze → Silver → Gold) implemented on Delta Lake / ADLS Gen2. The modelling strategy at each layer reflects a different concern:

- **Bronze**: Raw ingestion with no transformations. Data is stored as-landed, schema-on-read, append-only. Purpose is auditability and replayability.
- **Silver**: Cleaned, deduplicated, and conformed entity tables. Schemas are enforced. Surrogate keys are introduced. This layer serves as the single source of truth for downstream consumers.
- **Gold**: Dimensional model (star schema) optimised for analytical queries and BI reporting. Aggregated marts are built on top of the core star schema for specific analytical use cases.

The dimensional model in Gold is intentionally kept in **3NF-light star schema** form — dimension tables are not snowflaked unless there is a clear cardinality or reuse argument. This keeps SQL readable and join paths short, which matters in Databricks SQL / Power BI Direct Query scenarios.

---

## 2. Core Business Entities

The platform models a **retail / eCommerce operation** with the following core business entities:

| Entity                | Description                                                         |
|-----------------------|---------------------------------------------------------------------|
| `customers`           | Individuals or organisations that place orders                      |
| `products`            | SKU-level product catalogue with category and pricing attributes    |
| `orders`              | Order headers: date, channel, status, customer reference            |
| `order_items`         | Line-level detail: product, quantity, unit price, discount applied  |
| `inventory_snapshots` | Daily stock-on-hand snapshot per product and warehouse location     |
| `returns`             | Return events linked to original order lines                        |
| `channels`            | Sales channels (web, mobile, marketplace, in-store)                 |

These entities map directly to source system tables ingested via Azure Data Factory.

---

## 3. Bronze Layer Datasets

Bronze stores raw extracts without modification. Each dataset is partitioned by ingestion date (`ingest_date`). A `_metadata` struct captures source file name, pipeline run ID, and load timestamp for lineage.

| Bronze Dataset                   | Source Type                          | Load Pattern                             | Notes                                       |
|----------------------------------|--------------------------------------|------------------------------------------|---------------------------------------------|
| `bronze.raw_customers`           | OLTP database (CSV/Parquet export)   | Full + incremental                       | SCD candidate; includes PII fields          |
| `bronze.raw_products`            | Product catalogue API (JSON)         | Full refresh daily                       | Nested attributes flattened in Silver       |
| `bronze.raw_orders`              | OLTP database                        | Incremental (watermark on `updated_at`)  | Status field changes over time              |
| `bronze.raw_order_items`         | OLTP database                        | Incremental                              | Immutable after order is placed             |
| `bronze.raw_inventory_snapshots` | Warehouse WMS (CSV)                  | Daily full snapshot                      | One row per SKU per location per day        |
| `bronze.raw_returns`             | Returns management system (CSV)      | Incremental                              | Linked to `order_item_id`                   |
| `bronze.raw_channels`            | Static reference file (CSV)          | Full refresh on change                   | Rarely updated                              |

All Bronze tables are Delta format and retain history indefinitely (VACUUM is not run on Bronze).

---

## 4. Silver Layer Datasets

Silver applies the following transformations on top of Bronze:
- Schema enforcement and type casting
- Deduplication (row hash or primary key-based)
- Null handling and default value standardisation
- Surrogate key generation (SHA-256 hash of natural key)
- SCD Type 2 for slowly changing dimensions (customers, products)
- Referential integrity validation (records failing FK checks are quarantined)

| Silver Dataset                | Grain                                  | Key Type                  | SCD Strategy              |
|-------------------------------|----------------------------------------|---------------------------|---------------------------|
| `silver.customers`            | One row per customer version           | `customer_sk` (surrogate) | SCD Type 2                |
| `silver.products`             | One row per product version            | `product_sk` (surrogate)  | SCD Type 2                |
| `silver.orders`               | One row per order                      | `order_sk` (surrogate)    | Overwrite current state   |
| `silver.order_items`          | One row per order line                 | `order_item_sk`           | Immutable                 |
| `silver.inventory_snapshots`  | One row per SKU / location / date      | Composite natural key     | Daily append              |
| `silver.returns`              | One row per return event               | `return_sk`               | Immutable                 |
| `silver.channels`             | One row per channel                    | `channel_sk`              | SCD Type 1                |

Silver tables are the authoritative record used by Gold. No Gold table reads from Bronze.

---

## 5. Gold Layer Model

The Gold layer implements a **star schema** optimised for reporting and self-service analytics. The central fact table is `fact_order_items`, which captures revenue and quantity at the line level. Supporting fact tables cover inventory and returns.

```
                        ┌─────────────────┐
                        │   dim_date      │
                        └────────┬────────┘
                                 │
┌──────────────┐     ┌───────────▼──────────┐     ┌──────────────────┐
│ dim_customer │────▶│  fact_order_items    │◀────│   dim_product    │
└──────────────┘     └───────────┬──────────┘     └──────────────────┘
                                 │
                        ┌────────▼────────┐
                        │  dim_channel    │
                        └─────────────────┘
```

---

## 6. Fact Tables

### `gold.fact_order_items`

**Grain**: One row per order line item.

| Column           | Type    | Description                                          |
|------------------|---------|------------------------------------------------------|
| `order_item_sk`  | STRING  | Surrogate key (from Silver)                          |
| `order_sk`       | STRING  | FK → order header (degenerate dimension)             |
| `customer_sk`    | STRING  | FK → `dim_customer`                                  |
| `product_sk`     | STRING  | FK → `dim_product`                                   |
| `channel_sk`     | STRING  | FK → `dim_channel`                                   |
| `order_date_key` | INT     | FK → `dim_date` (YYYYMMDD)                           |
| `quantity`       | INT     | Units ordered                                        |
| `unit_price`     | DECIMAL | Listed price at time of order                        |
| `discount_amount`| DECIMAL | Total discount applied to the line                   |
| `gross_revenue`  | DECIMAL | `quantity × unit_price`                              |
| `net_revenue`    | DECIMAL | `gross_revenue − discount_amount`                    |
| `is_returned`    | BOOLEAN | Whether this line has an associated return           |
| `_batch_id`      | STRING  | Pipeline run identifier for lineage                  |

---

### `gold.fact_inventory_daily`

**Grain**: One row per product SKU, per warehouse location, per calendar day.

| Column              | Type    | Description                                         |
|---------------------|---------|-----------------------------------------------------|
| `snapshot_date_key` | INT     | FK → `dim_date`                                     |
| `product_sk`        | STRING  | FK → `dim_product`                                  |
| `location_id`       | STRING  | Warehouse / fulfilment centre identifier            |
| `stock_on_hand`     | INT     | Units available at close of day                     |
| `units_received`    | INT     | Units received during the day                       |
| `units_sold`        | INT     | Units fulfilled during the day                      |
| `units_returned`    | INT     | Units returned during the day                       |

---

### `gold.fact_returns`

**Grain**: One row per return event.

| Column           | Type    | Description                                          |
|------------------|---------|------------------------------------------------------|
| `return_sk`      | STRING  | Surrogate key                                        |
| `order_item_sk`  | STRING  | FK → `fact_order_items`                              |
| `customer_sk`    | STRING  | FK → `dim_customer`                                  |
| `product_sk`     | STRING  | FK → `dim_product`                                   |
| `return_date_key`| INT     | FK → `dim_date`                                      |
| `return_reason`  | STRING  | Reason code (defective, wrong item, changed mind…)   |
| `refund_amount`  | DECIMAL | Amount refunded to customer                          |

---

## 7. Dimension Tables

### `gold.dim_customer`

SCD Type 2. Each version of a customer record gets its own row.

| Column               | Type    | Description                                         |
|----------------------|---------|-----------------------------------------------------|
| `customer_sk`        | STRING  | Surrogate key                                       |
| `customer_id`        | STRING  | Natural key from source system                      |
| `full_name`          | STRING  | Masked / hashed for PII compliance in non-prod      |
| `email_domain`       | STRING  | Domain portion only (PII-safe)                      |
| `country`            | STRING  | Country of registration                             |
| `city`               | STRING  |                                                     |
| `customer_segment`   | STRING  | Derived segment: new, returning, high-value         |
| `registration_date`  | DATE    |                                                     |
| `valid_from`         | DATE    | SCD effective start date                            |
| `valid_to`           | DATE    | SCD effective end date (NULL = current)             |
| `is_current`         | BOOLEAN |                                                     |

---

### `gold.dim_product`

SCD Type 2. Tracks price and category changes over time.

| Column         | Type    | Description                                              |
|----------------|---------|----------------------------------------------------------|
| `product_sk`   | STRING  | Surrogate key                                            |
| `product_id`   | STRING  | Natural key (SKU)                                        |
| `product_name` | STRING  |                                                          |
| `category`     | STRING  | Top-level category (Electronics, Apparel…)               |
| `subcategory`  | STRING  |                                                          |
| `brand`        | STRING  |                                                          |
| `list_price`   | DECIMAL | Current list price at time of record version             |
| `cost_price`   | DECIMAL | Unit cost (used for margin analysis)                     |
| `is_active`    | BOOLEAN | Whether SKU is currently sold                            |
| `valid_from`   | DATE    |                                                          |
| `valid_to`     | DATE    |                                                          |
| `is_current`   | BOOLEAN |                                                          |

---

### `gold.dim_date`

Static date dimension pre-populated for a 10-year window. No FK to Silver.

| Column              | Type    | Description                                         |
|---------------------|---------|-----------------------------------------------------|
| `date_key`          | INT     | YYYYMMDD integer key                                |
| `full_date`         | DATE    |                                                     |
| `day_of_week`       | STRING  | Monday … Sunday                                     |
| `is_weekend`        | BOOLEAN |                                                     |
| `week_of_year`      | INT     |                                                     |
| `month`             | INT     |                                                     |
| `month_name`        | STRING  |                                                     |
| `quarter`           | INT     |                                                     |
| `year`              | INT     |                                                     |
| `is_public_holiday` | BOOLEAN | Configurable per market                             |

---

### `gold.dim_channel`

SCD Type 1. Small, stable reference table.

| Column         | Type   | Description                                              |
|----------------|--------|----------------------------------------------------------|
| `channel_sk`   | STRING | Surrogate key                                            |
| `channel_id`   | STRING | Natural key                                              |
| `channel_name` | STRING | Web, Mobile App, Marketplace, In-Store                   |
| `channel_type` | STRING | Online / Offline                                         |

---

## 8. Analytical Outputs / Gold Marts

Pre-aggregated mart tables expose summarised metrics for BI tools and ad-hoc SQL users. These are rebuilt on a scheduled cadence (daily) and do not replace the core star schema — they accelerate common query patterns.

| Mart Table                       | Description                                                    | Refresh |
|----------------------------------|----------------------------------------------------------------|---------|
| `gold.mart_sales_daily`          | Revenue, quantity, order count by channel and date             | Daily   |
| `gold.mart_product_performance`  | Gross/net revenue, return rate, margin by product and month    | Daily   |
| `gold.mart_customer_cohorts`     | Monthly cohort retention based on first order date             | Weekly  |
| `gold.mart_inventory_alerts`     | Products with stock-on-hand below reorder threshold            | Daily   |
| `gold.mart_returns_summary`      | Return rate and refund volume by reason code, category, channel | Daily  |

These marts are the primary targets for Power BI reports and executive dashboards.

---

## 9. Grain and Modelling Notes

- **Fact grain is line-level** (`fact_order_items`), not order-level. This allows flexible roll-up to order, customer, product, or channel without pre-aggregation loss.
- **Revenue metrics** (`gross_revenue`, `net_revenue`) are pre-computed at the fact level to avoid repeated expression logic in BI tools. They are additive across all dimensions.
- **Returns are modelled as a separate fact**, not as negative order lines, to preserve analytical clarity and support reason-code analysis independently of sales analysis.
- **Inventory is a snapshot fact** (non-additive across dates). Summing `stock_on_hand` across multiple dates is meaningless; mart queries use `MAX` or point-in-time filters.
- **SCD Type 2 on customers and products** allows historical accuracy: an order from 18 months ago reflects the customer segment and product price at the time of that order.
- **PII handling**: `dim_customer` stores only non-identifying fields in production. Full name is masked; email is domain-only. Raw PII remains in Bronze under access-controlled storage.
- **Degenerate dimensions**: `order_sk` is retained in `fact_order_items` to support order-header-level grouping without a separate `dim_order` table, which would add a join with minimal analytical value.

---

## 10. Scope Boundaries

The following are explicitly **out of scope** for this platform:

- **Campaign / marketing attribution**: No campaign spend or impression data is ingested. Channel dimension identifies the sales channel but does not model paid media.
- **Real-time / streaming**: All pipelines are batch. The minimum latency is daily. Near-real-time use cases would require a separate streaming layer (e.g., Event Hubs + Structured Streaming).
- **Financial consolidation**: Revenue figures reflect gross/net sales as reported by the order system. They are not reconciled against accounting or ERP data.
- **Multi-currency**: All monetary values are stored in a single base currency. Currency conversion is not modelled.
- **Customer lifetime value (CLV) scoring**: Cohort retention is provided as a mart; predictive CLV modelling is outside the scope of this platform.
- **Product recommendation**: No ML feature store or recommendation output is included.

These boundaries are deliberate. The platform is scoped to **descriptive and diagnostic analytics** on retail operations data, which is the primary decision-support need for the target business context.
