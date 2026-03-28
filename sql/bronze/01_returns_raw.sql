CREATE OR REPLACE TABLE lakehouse_prod.bronze.returns_raw
USING DELTA
AS
SELECT
  CAST(return_id AS STRING) AS return_id,
  CAST(return_date AS DATE) AS return_date,
  CAST(order_item_id AS STRING) AS order_item_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(return_reason AS STRING) AS return_reason,
  CAST(refund_amount AS DECIMAL(10,2)) AS refund_amount,
  _rescued_data
FROM read_files(
  'abfss://bronze@stazlakeetlweu01.dfs.core.windows.net/azure-lakehouse-etl/seed/returns.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);
