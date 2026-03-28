CREATE OR REPLACE TABLE lakehouse_prod.silver.customers_clean
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
FROM lakehouse_prod.bronze.customers_raw
WHERE _rescued_data IS NULL;
