CREATE OR REPLACE TABLE lakehouse_prod.gold.model_contract_v1
USING DELTA
AS
SELECT 'fact_sales' AS object_name, 'TABLE' AS object_type, '1 row per order_item_id' AS declared_grain, 'gold' AS layer
UNION ALL
SELECT 'dim_customers', 'TABLE', '1 row per customer_id', 'gold'
UNION ALL
SELECT 'dim_products', 'TABLE', '1 row per product_id', 'gold'
UNION ALL
SELECT 'fact_returns_enriched', 'TABLE', '1 row per return_id', 'gold'
UNION ALL
SELECT 'v_sales_enriched', 'VIEW', '1 row per order_item_id', 'gold'
UNION ALL
SELECT 'v_returns_enriched', 'VIEW', '1 row per return_id', 'gold'
UNION ALL
SELECT 'v_dq_status', 'VIEW', '1 row per check_name', 'gold';
