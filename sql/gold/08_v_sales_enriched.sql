-- grain: 1 row per order_item_id
CREATE OR REPLACE VIEW {{catalog}}.gold.v_sales_enriched
AS
SELECT
  s.order_item_id,
  s.order_id,
  s.order_date,
  s.channel,
  s.customer_id,
  c.first_name,
  c.last_name,
  c.city,
  c.country,
  c.customer_status,
  s.product_id,
  p.product_name,
  p.brand,
  p.category,
  p.subcategory,
  s.quantity,
  s.unit_price,
  s.discount_amount,
  s.line_total,
  s.currency,
  s.order_status
FROM {{catalog}}.gold.fact_sales s
LEFT JOIN {{catalog}}.gold.dim_customers c
  ON s.customer_id = c.customer_id
LEFT JOIN {{catalog}}.gold.dim_products p
  ON s.product_id = p.product_id;
