CREATE OR REPLACE TABLE lakehouse_prod.gold.returns_by_product
USING DELTA
AS
SELECT
  product_id,
  COUNT(*) AS total_returns,
  SUM(refund_amount) AS total_refund_amount
FROM lakehouse_prod.gold.fact_returns_enriched
GROUP BY product_id;
