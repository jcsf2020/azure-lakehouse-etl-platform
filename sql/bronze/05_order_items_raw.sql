CREATE OR REPLACE TABLE lakehouse_prod.bronze.order_items_raw
USING DELTA
AS
SELECT
  CAST(order_item_id AS STRING) AS order_item_id,
  CAST(order_id AS STRING) AS order_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
  CAST(discount_amount AS DECIMAL(10,2)) AS discount_amount,
  CAST(line_total AS DECIMAL(10,2)) AS line_total,
  _rescued_data
FROM read_files(
  'abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/order_items.json',
  format => 'json',
  multiLine => true
);
