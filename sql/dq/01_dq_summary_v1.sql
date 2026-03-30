CREATE OR REPLACE TABLE lakehouse_prod.gold.dq_summary_v1
USING DELTA
AS
SELECT
  'dim_customers_duplicate_customer_id' AS check_name,
  COUNT(*) - COUNT(DISTINCT customer_id) AS issue_count
FROM lakehouse_prod.gold.dim_customers

UNION ALL

SELECT
  'dim_customers_null_customer_id',
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)
FROM lakehouse_prod.gold.dim_customers

UNION ALL

SELECT
  'dim_products_duplicate_product_id',
  COUNT(*) - COUNT(DISTINCT product_id)
FROM lakehouse_prod.gold.dim_products

UNION ALL

SELECT
  'dim_products_null_product_id',
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
FROM lakehouse_prod.gold.dim_products

UNION ALL

SELECT
  'fact_sales_duplicate_order_item_id',
  COUNT(*) - COUNT(DISTINCT order_item_id)
FROM lakehouse_prod.gold.fact_sales

UNION ALL

SELECT
  'fact_sales_null_order_item_id',
  SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END)
FROM lakehouse_prod.gold.fact_sales

UNION ALL

SELECT
  'fact_returns_unmatched_sales',
  SUM(CASE WHEN reconciliation_status = 'UNMATCHED' THEN 1 ELSE 0 END)
FROM lakehouse_prod.gold.fact_returns_enriched;
