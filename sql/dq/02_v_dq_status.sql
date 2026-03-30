CREATE OR REPLACE VIEW lakehouse_prod.gold.v_dq_status AS
SELECT
  check_name,
  issue_count,
  CASE
    WHEN issue_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS check_status
FROM lakehouse_prod.gold.dq_summary_v1;
