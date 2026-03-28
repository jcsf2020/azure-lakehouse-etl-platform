-- grain: 1 row per customer_id
CREATE OR REPLACE TABLE lakehouse_prod.gold.dim_customers
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
FROM lakehouse_prod.silver.customers_clean;
