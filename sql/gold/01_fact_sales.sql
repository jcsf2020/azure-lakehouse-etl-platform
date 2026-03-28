-- grain: 1 row per order_item_id
CREATE OR REPLACE TABLE lakehouse_prod.gold.fact_sales
USING DELTA
AS
SELECT
  oi.order_item_id,
  oi.order_id,
  o.order_date,
  o.channel,
  o.customer_id,
  oi.product_id,
  oi.quantity,
  oi.unit_price,
  oi.discount_amount,
  oi.line_total,
  o.currency,
  o.order_status
FROM lakehouse_prod.silver.order_items_clean oi
INNER JOIN lakehouse_prod.silver.orders_clean o
  ON oi.order_id = o.order_id;
