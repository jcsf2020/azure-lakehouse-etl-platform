CREATE OR REPLACE TABLE {{catalog}}.silver.order_items_clean
USING DELTA
AS
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  discount_amount,
  line_total
FROM {{catalog}}.bronze.order_items_raw
WHERE _rescued_data IS NULL;
