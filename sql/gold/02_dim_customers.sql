-- grain: 1 row per customer_id
CREATE OR REPLACE TABLE {{catalog}}.gold.dim_customers
USING DELTA
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  country,
  customer_status,
  registration_date
FROM {{catalog}}.silver.customers_clean;
