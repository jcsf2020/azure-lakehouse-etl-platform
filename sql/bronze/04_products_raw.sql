CREATE OR REPLACE TABLE {{catalog}}.bronze.products_raw
USING DELTA
AS
SELECT
  CAST(product_id AS STRING) AS product_id,
  CAST(product_name AS STRING) AS product_name,
  CAST(category AS STRING) AS category,
  CAST(subcategory AS STRING) AS subcategory,
  CAST(brand AS STRING) AS brand,
  CAST(list_price AS DECIMAL(10,2)) AS list_price,
  CAST(cost_price AS DECIMAL(10,2)) AS cost_price,
  CAST(currency AS STRING) AS currency,
  CAST(is_active AS BOOLEAN) AS is_active,
  CAST(last_updated AS TIMESTAMP) AS last_updated,
  _rescued_data
FROM read_files(
  'abfss://bronze@{{storage_account}}.dfs.core.windows.net/azure-lakehouse-etl/seed/products.json',
  format => 'json',
  multiLine => true
);
