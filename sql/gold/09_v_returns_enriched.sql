-- grain: 1 row per return_id
CREATE OR REPLACE VIEW lakehouse_prod.gold.v_returns_enriched
AS
SELECT
  r.return_id,
  r.return_date,
  r.order_item_id,
  r.reconciliation_status,
  r.order_id,
  r.order_date,
  r.channel,
  r.customer_id,
  c.first_name,
  c.last_name,
  c.city,
  c.country,
  c.customer_status,
  r.product_id,
  p.product_name,
  p.brand,
  p.category,
  p.subcategory,
  r.return_reason,
  r.refund_amount,
  r.unit_price,
  r.discount_amount,
  r.line_total
FROM lakehouse_prod.gold.fact_returns_enriched r
LEFT JOIN lakehouse_prod.gold.dim_customers c
  ON r.customer_id = c.customer_id
LEFT JOIN lakehouse_prod.gold.dim_products p
  ON r.product_id = p.product_id;
