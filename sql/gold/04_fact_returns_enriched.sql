-- grain: 1 row per return_id
CREATE OR REPLACE TABLE lakehouse_prod.gold.fact_returns_enriched
USING DELTA
AS
SELECT
  r.return_id,
  r.return_date,
  r.order_item_id,
  s.order_id,
  s.order_date,
  s.channel,
  r.customer_id,
  r.product_id,
  r.return_reason,
  r.refund_amount,
  s.unit_price,
  s.discount_amount,
  s.line_total,
  CASE
    WHEN s.order_id IS NULL THEN 'UNMATCHED'
    ELSE 'MATCHED'
  END AS reconciliation_status
FROM lakehouse_prod.silver.returns_clean r
LEFT JOIN lakehouse_prod.gold.fact_sales s
  ON r.order_item_id = s.order_item_id;
