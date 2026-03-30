CREATE OR REPLACE TABLE lakehouse_prod.bronze.orders_raw
USING DELTA
AS
SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(order_date AS TIMESTAMP) AS order_date,
  CAST(channel AS STRING) AS channel,
  CAST(order_status AS STRING) AS order_status,
  CAST(currency AS STRING) AS currency,
  CAST(order_total AS DECIMAL(10,2)) AS order_total,
  _rescued_data
FROM read_files(
  'abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/orders.json',
  format => 'json',
  multiLine => true
);
