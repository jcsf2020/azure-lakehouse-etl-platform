CREATE OR REPLACE TABLE {{catalog}}.gold.returns_reason_summary
USING DELTA
AS
SELECT
  return_reason,
  COUNT(*) AS total_returns,
  SUM(refund_amount) AS total_refund_amount,
  AVG(refund_amount) AS avg_refund_amount
FROM {{catalog}}.silver.returns_clean
GROUP BY return_reason;
