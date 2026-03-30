# Data Model

## 1. Modelling Approach

This platform follows a **medallion architecture** (Bronze → Silver → Gold) implemented on Delta Lake / ADLS Gen2. Each layer has a distinct modelling concern:

- **Bronze**: Raw ingestion. No transformations. `_rescued_data` captures schema violations. Append-on-ingest, no history pruning.
- **Silver**: Cleaned and typed entity tables. Rows with schema violations are filtered out. Natural keys are preserved. No surrogate keys.
- **Gold**: SQL-driven serving layer. Dimensional tables, fact tables, aggregation tables, enrichment views, data quality objects, and a model contract — all defined as `CREATE OR REPLACE TABLE/VIEW` and executed in Databricks.

Dimension tables use **Type 1 (overwrite)** — the current state of customers and products is reflected; historical versioning is not implemented. Fact tables use natural keys from Silver. No surrogate key generation exists in the SQL asset layer.

---

## 2. Core Business Entities

| Entity         | Description                                                         |
|----------------|---------------------------------------------------------------------|
| `customers`    | Individuals or organisations that place orders                      |
| `products`     | SKU-level product catalogue with category and pricing attributes    |
| `orders`       | Order headers: date, channel, status, customer reference            |
| `order_items`  | Line-level detail: product, quantity, unit price, discount applied  |
| `returns`      | Return events linked to original order lines                        |

These entities map directly to the five source files ingested via Azure Data Factory into Bronze.

---

## 3. Bronze Layer — `lakehouse_prod.bronze`

Five raw tables. Schema inferred from source files. `_rescued_data` captures any field that does not conform to the inferred schema.

| Table              | Source File        | Format | Primary Key      |
|--------------------|--------------------|--------|------------------|
| `orders_raw`       | `orders.json`      | JSON   | `order_id`       |
| `order_items_raw`  | `order_items.json` | JSON   | `order_item_id`  |
| `customers_raw`    | `customers.json`   | JSON   | `customer_id`    |
| `products_raw`     | `products.json`    | JSON   | `product_id`     |
| `returns_raw`      | `returns.csv`      | CSV    | `return_id`      |

Source base path: `abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/`

All Bronze tables are Delta format. `_rescued_data` is retained to preserve malformed field values for audit.

---

## 4. Silver Layer — `lakehouse_prod.silver`

Five cleaned tables. Each script applies `WHERE _rescued_data IS NULL` to exclude malformed records and drops the rescue column. No further transformations are applied at this layer; natural keys and original data types are preserved from Bronze.

| Table               | Grain                  | Key Field        | SCD Strategy          |
|---------------------|------------------------|------------------|-----------------------|
| `orders_clean`      | 1 row per order        | `order_id`       | Overwrite (full load) |
| `order_items_clean` | 1 row per order line   | `order_item_id`  | Overwrite (full load) |
| `customers_clean`   | 1 row per customer     | `customer_id`    | Overwrite (full load) |
| `products_clean`    | 1 row per product      | `product_id`     | Overwrite (full load) |
| `returns_clean`     | 1 row per return event | `return_id`      | Overwrite (full load) |

Silver tables are the exclusive source for Gold. No Gold object reads from Bronze.

---

## 5. Gold Layer — `lakehouse_prod.gold`

All Gold objects are SQL-defined. The serving layer is Databricks SQL; there is no Python transformation at Gold. The diagram below shows the key relationships:

```
                    ┌───────────────────┐
                    │   dim_customers   │
                    └────────┬──────────┘
                             │ customer_id
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
  ┌──────────────┐   ┌────────────────┐  ┌─────────────────────────┐
  │  fact_sales  │   │  dim_products  │  │  fact_returns_enriched  │
  │ (order_item  │   └────────────────┘  │  (return_id grain)      │
  │   grain)     │          │            │  LEFT JOIN → fact_sales  │
  └──────┬───────┘          │ product_id └─────────────────────────┘
         │                  │
         ▼                  ▼
  ┌──────────────┐   ┌────────────────────────────────┐
  │v_sales_enr.  │   │  orders_channel_summary         │
  │v_returns_enr.│   │  returns_reason_summary         │
  └──────────────┘   │  returns_by_product             │
                     └────────────────────────────────┘
```

---

## 6. Fact Tables

### `gold.fact_sales`

**Grain**: One row per `order_item_id`.

| Column           | Type           | Description                                           |
|------------------|----------------|-------------------------------------------------------|
| `order_item_id`  | STRING         | Natural key — primary identifier for the line         |
| `order_id`       | STRING         | Degenerate dimension — order header reference         |
| `order_date`     | TIMESTAMP      | Order placement timestamp                             |
| `channel`        | STRING         | Sales channel (web, mobile, marketplace, in-store)    |
| `customer_id`    | STRING         | FK → `dim_customers`                                  |
| `product_id`     | STRING         | FK → `dim_products`                                   |
| `quantity`       | INT            | Units ordered                                         |
| `unit_price`     | DECIMAL(10,2)  | Price per unit at time of order                       |
| `discount_amount`| DECIMAL(10,2)  | Discount applied to the line                          |
| `line_total`     | DECIMAL(10,2)  | `quantity × unit_price − discount_amount`             |
| `currency`       | STRING         | Currency code                                         |
| `order_status`   | STRING         | Order status at time of processing                    |

**Build**: INNER JOIN `silver.order_items_clean` on `silver.orders_clean` via `order_id`.

---

### `gold.fact_returns_enriched`

**Grain**: One row per `return_id`.

| Column                  | Type           | Description                                                   |
|-------------------------|----------------|---------------------------------------------------------------|
| `return_id`             | STRING         | Natural key — primary identifier for the return event         |
| `return_date`           | DATE           | Date the return was processed                                 |
| `order_item_id`         | STRING         | FK → `fact_sales` (nullable when UNMATCHED)                   |
| `order_id`              | STRING         | From LEFT JOIN to `fact_sales` — null when unmatched          |
| `order_date`            | TIMESTAMP      | From LEFT JOIN to `fact_sales` — null when unmatched          |
| `channel`               | STRING         | From LEFT JOIN to `fact_sales` — null when unmatched          |
| `customer_id`           | STRING         | FK → `dim_customers`                                          |
| `product_id`            | STRING         | FK → `dim_products`                                           |
| `return_reason`         | STRING         | Reason code for the return                                    |
| `refund_amount`         | DECIMAL(10,2)  | Amount refunded to customer                                   |
| `unit_price`            | DECIMAL(10,2)  | From LEFT JOIN to `fact_sales` — null when unmatched          |
| `discount_amount`       | DECIMAL(10,2)  | From LEFT JOIN to `fact_sales` — null when unmatched          |
| `line_total`            | DECIMAL(10,2)  | From LEFT JOIN to `fact_sales` — null when unmatched          |
| `reconciliation_status` | STRING         | `'MATCHED'` or `'UNMATCHED'`                                  |

**Build**: LEFT JOIN `silver.returns_clean` to `gold.fact_sales` on `order_item_id`. Returns with no matching sale record receive `reconciliation_status = 'UNMATCHED'` and null values for all `fact_sales` join columns. This is intentional — unmatched returns are surfaced, not discarded. Validated count of UNMATCHED: 3 (expected by design).

---

## 7. Dimension Tables

### `gold.dim_customers`

**Grain**: One row per `customer_id`. Type 1 — current state only.

| Column              | Type    | Description                   |
|---------------------|---------|-------------------------------|
| `customer_id`       | STRING  | Natural key from source system |
| `first_name`        | STRING  | Given name                    |
| `last_name`         | STRING  | Family name                   |
| `email`             | STRING  | Email address                 |
| `city`              | STRING  | City of residence             |
| `country`           | STRING  | Country of residence          |
| `customer_status`   | STRING  | Active / inactive             |
| `registration_date` | DATE    | Account creation date         |

**Build**: Direct `CREATE OR REPLACE TABLE ... AS SELECT` from `silver.customers_clean`. No joins.

---

### `gold.dim_products`

**Grain**: One row per `product_id`. Type 1 — current state only.

| Column          | Type           | Description                             |
|-----------------|----------------|-----------------------------------------|
| `product_id`    | STRING         | Natural key (SKU)                       |
| `product_name`  | STRING         | Product description                     |
| `category`      | STRING         | Top-level category                      |
| `subcategory`   | STRING         | Sub-category                            |
| `brand`         | STRING         | Brand name                              |
| `list_price`    | DECIMAL(10,2)  | Recommended retail price                |
| `cost_price`    | DECIMAL(10,2)  | Unit cost                               |
| `currency`      | STRING         | Currency code                           |
| `is_active`     | BOOLEAN        | Whether the SKU is currently sold       |
| `last_updated`  | TIMESTAMP      | Last update timestamp from source       |

**Build**: Direct `CREATE OR REPLACE TABLE ... AS SELECT` from `silver.products_clean`. No joins.

---

## 8. Aggregation Tables

Pre-aggregated tables expose summarised metrics for BI tools and ad hoc SQL consumers. Built as `CREATE OR REPLACE TABLE` — fully refreshed on each run.

### `gold.orders_channel_summary`

**Grain**: One row per `channel`.

| Column            | Type    | Description                                |
|-------------------|---------|--------------------------------------------|
| `channel`         | STRING  | Sales channel                              |
| `total_orders`    | BIGINT  | Count of orders                            |
| `total_revenue`   | DECIMAL | Sum of `order_total`                       |
| `avg_order_value` | DECIMAL | Average order total per channel            |

**Source**: `silver.orders_clean`, grouped by `channel`.

---

### `gold.returns_reason_summary`

**Grain**: One row per `return_reason`.

| Column               | Type    | Description                               |
|----------------------|---------|-------------------------------------------|
| `return_reason`      | STRING  | Return reason code                        |
| `total_returns`      | BIGINT  | Count of return events                    |
| `total_refund_amount`| DECIMAL | Sum of refund amounts                     |
| `avg_refund_amount`  | DECIMAL | Average refund amount per return          |

**Source**: `silver.returns_clean`, grouped by `return_reason`.

---

### `gold.returns_by_product`

**Grain**: One row per `product_id`.

| Column               | Type    | Description                              |
|----------------------|---------|------------------------------------------|
| `product_id`         | STRING  | Product natural key                      |
| `total_returns`      | BIGINT  | Count of return events for this product  |
| `total_refund_amount`| DECIMAL | Sum of refunds for this product          |

**Source**: `gold.fact_returns_enriched`, grouped by `product_id`.

---

## 9. Enrichment Views

Views provide pre-joined, self-service access combining fact data with dimensional context. Both use LEFT JOINs to dimensions, ensuring fact rows are never dropped if a dimension record is missing.

### `gold.v_sales_enriched`

**Grain**: One row per `order_item_id` (same as `fact_sales`).

All columns from `fact_sales` plus:
- `first_name`, `last_name`, `city`, `country`, `customer_status` — from `dim_customers`
- `product_name`, `brand`, `category`, `subcategory` — from `dim_products`

---

### `gold.v_returns_enriched`

**Grain**: One row per `return_id` (same as `fact_returns_enriched`).

All columns from `fact_returns_enriched` plus:
- `first_name`, `last_name`, `city`, `country`, `customer_status` — from `dim_customers`
- `product_name`, `brand`, `category`, `subcategory` — from `dim_products`

---

## 10. Data Quality Layer

### `gold.dq_summary_v1`

**Grain**: One row per check name.

| Column        | Type   | Description                                         |
|---------------|--------|-----------------------------------------------------|
| `check_name`  | STRING | Identifier for the quality check                    |
| `issue_count` | BIGINT | Number of violations found (0 = passing)            |

Checks implemented:

| Check Name                            | Logic                                                        |
|---------------------------------------|--------------------------------------------------------------|
| `dim_customers_duplicate_customer_id` | `COUNT(*) - COUNT(DISTINCT customer_id)` in `dim_customers`  |
| `dim_customers_null_customer_id`      | Count of NULL `customer_id` in `dim_customers`               |
| `dim_products_duplicate_product_id`   | `COUNT(*) - COUNT(DISTINCT product_id)` in `dim_products`    |
| `dim_products_null_product_id`        | Count of NULL `product_id` in `dim_products`                 |
| `fact_sales_duplicate_order_item_id`  | `COUNT(*) - COUNT(DISTINCT order_item_id)` in `fact_sales`   |
| `fact_sales_null_order_item_id`       | Count of NULL `order_item_id` in `fact_sales`                |
| `fact_returns_unmatched_sales`        | Count of rows where `reconciliation_status = 'UNMATCHED'`    |

### `gold.v_dq_status`

**Grain**: One row per check name.

| Column          | Type   | Description                                   |
|-----------------|--------|-----------------------------------------------|
| `check_name`    | STRING | Check identifier                              |
| `issue_count`   | BIGINT | Issue count from `dq_summary_v1`              |
| `check_status`  | STRING | `'PASS'` if `issue_count = 0`, else `'FAIL'`  |

**Validated results**: All checks PASS. `fact_returns_unmatched_sales = 3` — status is FAIL by strict definition, but this count is expected and intentional given the LEFT JOIN design of `fact_returns_enriched`.

---

## 11. Model Contract

### `gold.model_contract_v1`

**Grain**: One row per declared Gold object.

| Column           | Type   | Description                                        |
|------------------|--------|----------------------------------------------------|
| `object_name`    | STRING | Table or view name                                 |
| `object_type`    | STRING | `'TABLE'` or `'VIEW'`                              |
| `declared_grain` | STRING | Human-readable grain declaration                   |
| `layer`          | STRING | Medallion layer (`gold`)                           |

Declared objects:

| Object Name              | Type  | Declared Grain                  |
|--------------------------|-------|---------------------------------|
| `fact_sales`             | TABLE | 1 row per `order_item_id`       |
| `dim_customers`          | TABLE | 1 row per `customer_id`         |
| `dim_products`           | TABLE | 1 row per `product_id`          |
| `fact_returns_enriched`  | TABLE | 1 row per `return_id`           |
| `v_sales_enriched`       | VIEW  | 1 row per `order_item_id`       |
| `v_returns_enriched`     | VIEW  | 1 row per `return_id`           |
| `v_dq_status`            | VIEW  | 1 row per check name            |

---

## 12. Modelling Notes

- **Fact grain is line-level** (`fact_sales` is at `order_item_id`, not `order_id`). Revenue can be rolled up to order, customer, product, channel, or date without pre-aggregation loss.
- **Returns use LEFT JOIN semantics intentionally.** `fact_returns_enriched` does not require a matching `fact_sales` record. Orphaned returns are captured as UNMATCHED and quantified in `dq_summary_v1`.
- **No SCD Type 2.** Dimensions reflect current state only. Historical customer or product attribute changes are not tracked in the SQL asset layer.
- **No surrogate keys.** All joins use natural keys from source systems (`customer_id`, `product_id`, `order_item_id`, `return_id`).
- **Aggregation tables use `fact_returns_enriched` as source, not Silver.** `returns_by_product` builds on the enriched fact, giving it access to reconciliation status if needed.

---

## 13. Scope Boundaries

Out of scope for this platform:

- **SCD Type 2 / historical dimension versioning** — not implemented in SQL assets
- **Surrogate key generation** — natural keys are used throughout
- **`dim_date` or `dim_channel`** — not present in the SQL asset layer
- **Inventory data model** — `inventory_snapshots` exists as seed data and Python library code but has no corresponding SQL asset in Bronze, Silver, or Gold
- **Campaign attribution or CLV scoring** — no ML or marketing attribution data
- **Multi-currency conversion** — all amounts stored in source currency
- **Streaming / near-real-time** — batch-only platform
