CREATE OR REPLACE TABLE {{catalog}}.silver.products_clean
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
FROM {{catalog}}.bronze.products_raw
WHERE _rescued_data IS NULL;
