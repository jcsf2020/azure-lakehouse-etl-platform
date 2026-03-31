CREATE OR REPLACE TABLE {{catalog}}.silver.orders_clean
USING DELTA
AS
SELECT
  order_id,
  customer_id,
  order_date,
  channel,
  order_status,
  currency,
  order_total
FROM {{catalog}}.bronze.orders_raw
WHERE _rescued_data IS NULL;
