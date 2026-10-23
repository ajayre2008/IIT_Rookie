-- include/sql/raw_load.sql
-- NammaMart Raw Layer Ingestion
-- Ingests source CSV files into raw tables without transformations
-- Exactly as received, all columns as text, plus _loaded_at, _source_file, and _run_id

CREATE OR REPLACE TABLE raw_customers AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'customers.csv' AS _source_file,
    'morning_batch' AS _run_id
FROM read_csv_auto('include/data/nammamart/customers.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_products AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'products.csv' AS _source_file,
    'morning_batch' AS _run_id
FROM read_csv_auto('include/data/nammamart/products.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_stores AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'stores.csv' AS _source_file,
    'morning_batch' AS _run_id
FROM read_csv_auto('include/data/nammamart/stores.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_orders AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'orders.csv' AS _source_file,
    'morning_batch' AS _run_id
FROM read_csv_auto('include/data/nammamart/orders.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_payments AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'payments.csv' AS _source_file,
    'morning_batch' AS _run_id
FROM read_csv_auto('include/data/nammamart/payments.csv', all_varchar=true);

