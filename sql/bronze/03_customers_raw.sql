CREATE OR REPLACE TABLE {{catalog}}.bronze.customers_raw
USING DELTA
AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  CAST(first_name AS STRING) AS first_name,
  CAST(last_name AS STRING) AS last_name,
  CAST(email AS STRING) AS email,
  CAST(city AS STRING) AS city,
  CAST(country AS STRING) AS country,
  CAST(customer_status AS STRING) AS customer_status,
  CAST(registration_date AS DATE) AS registration_date,
  _rescued_data
FROM read_files(
  'abfss://bronze@{{storage_account}}.dfs.core.windows.net/azure-lakehouse-etl/seed/customers.json',
  format => 'json',
  multiLine => true
);
