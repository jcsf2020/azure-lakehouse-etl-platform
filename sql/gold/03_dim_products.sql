-- grain: 1 row per product_id
CREATE OR REPLACE TABLE lakehouse_prod.gold.dim_products
USING DELTA
AS
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  brand,
  list_price,
  cost_price,
  currency,
  is_active,
  last_updated
FROM lakehouse_prod.silver.products_clean;
