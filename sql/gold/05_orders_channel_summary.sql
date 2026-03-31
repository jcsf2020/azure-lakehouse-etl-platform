CREATE OR REPLACE TABLE {{catalog}}.gold.orders_channel_summary
USING DELTA
AS
SELECT
  channel,
  COUNT(*) AS total_orders,
  SUM(order_total) AS total_revenue,
  AVG(order_total) AS avg_order_value
FROM {{catalog}}.silver.orders_clean
GROUP BY channel;
