CREATE OR REPLACE TABLE lakehouse_prod.silver.products_clean
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
FROM lakehouse_prod.bronze.products_raw
WHERE _rescued_data IS NULL;
