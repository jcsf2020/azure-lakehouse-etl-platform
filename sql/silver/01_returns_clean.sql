CREATE OR REPLACE TABLE lakehouse_prod.silver.returns_clean
USING DELTA
AS
SELECT
  return_id,
  return_date,
  order_item_id,
  product_id,
  customer_id,
  return_reason,
  refund_amount
FROM lakehouse_prod.bronze.returns_raw
WHERE _rescued_data IS NULL;
