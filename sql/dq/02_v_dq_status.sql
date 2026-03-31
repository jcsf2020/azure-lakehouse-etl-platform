CREATE OR REPLACE VIEW {{catalog}}.gold.v_dq_status AS
SELECT
  check_name,
  issue_count,
  CASE
    WHEN issue_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS check_status
FROM {{catalog}}.gold.dq_summary_v1;
