-- include/sql/incremental_merge.sql
-- NammaMart Incremental Merge Layer (Day 2 batch: orders_2026-10-01_increment.csv)

CREATE OR REPLACE TABLE raw_orders_increment AS
SELECT * FROM read_csv_auto('include/data/nammamart/orders_2026-10-01_increment.csv', all_varchar=true);

CREATE OR REPLACE TEMP TABLE stg_orders_inc AS
WITH deduplicated AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        TRY_CAST(quantity AS BIGINT) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        UPPER(TRIM(payment_mode)) AS raw_pmode,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY order_ts ASC) AS rn
    FROM raw_orders_increment
    WHERE order_ts NOT LIKE '%31/09%'
)
SELECT
    o.order_id,
    o.order_ts,
    CAST(o.order_ts AS DATE) AS order_date,
    o.customer_id,
    o.store_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.order_amount,
    CASE
        WHEN o.raw_pmode IN ('COD', 'CASH ON DELIVERY') THEN 'Cash on Delivery'
        WHEN o.raw_pmode IN ('UPI') THEN 'UPI'
        WHEN o.raw_pmode IN ('CREDIT CARD', 'CC') THEN 'Credit Card'
        WHEN o.raw_pmode IN ('DEBIT CARD', 'DC') THEN 'Debit Card'
        WHEN o.raw_pmode IN ('NET BANKING', 'NETBANKING') THEN 'Net Banking'
        ELSE 'Other'
    END AS payment_mode,
    o.order_status,
    o.channel,
    o.delivery_minutes,
    p.cost_price,
    CASE
        WHEN o.order_status = 'DELIVERED' THEN ROUND(o.order_amount, 2)
        ELSE 0.0
    END AS net_revenue,
    CASE
        WHEN o.order_status = 'DELIVERED' THEN ROUND(o.order_amount - (o.quantity * p.cost_price), 2)
        ELSE 0.0
    END AS gross_margin,
    CASE
        WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 THEN true
        ELSE false
    END AS within_15_min,
    CURRENT_TIMESTAMP AS staged_at
FROM deduplicated o
JOIN stg_customers c ON o.customer_id = c.customer_id
JOIN stg_products p ON o.product_id = p.product_id
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.rn = 1
  AND o.order_ts IS NOT NULL
  AND CAST(o.order_ts AS DATE) <= DATE '2026-10-02'
  AND o.order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')
  AND o.quantity > 0
  AND o.discount_pct BETWEEN 0 AND 100
  AND (
      (o.order_status = 'DELIVERED' AND o.delivery_minutes BETWEEN 0 AND 120)
      OR o.order_status != 'DELIVERED'
  )
  AND ABS(o.order_amount - ROUND(o.quantity * o.unit_price * (1.0 - o.discount_pct / 100.0), 2)) <= 0.05;

-- Idempotent upsert: delete existing rows that are present in incremental feed, then insert
DELETE FROM stg_orders WHERE order_id IN (SELECT order_id FROM stg_orders_inc);
INSERT INTO stg_orders SELECT * FROM stg_orders_inc;
