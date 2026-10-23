-- include/sql/staging.sql
-- NammaMart Staging Layer Transformations

-- 1. Staging Customers
CREATE OR REPLACE TABLE stg_customers AS
WITH deduplicated AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(full_name) AS full_name,
        email,
        phone,
        TRIM(city) AS city,
        TRIM(area) AS area,
        TRY_CAST(pincode AS BIGINT) AS pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        CASE
            WHEN LOWER(TRIM(loyalty_tier)) IN ('gold', 'gld') THEN 'Gold'
            WHEN LOWER(TRIM(loyalty_tier)) IN ('platinum', 'plat') THEN 'Platinum'
            WHEN LOWER(TRIM(loyalty_tier)) IN ('silver', 'silv') THEN 'Silver'
            ELSE 'Standard'
        END AS loyalty_tier,
        TRY_CAST(is_active AS BOOLEAN) AS is_active,
        ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY signup_date ASC) AS rn
    FROM raw_customers
)
SELECT
    customer_id,
    full_name,
    CASE
        WHEN email IS NULL OR NOT regexp_matches(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'invalid@masked.com'
        ELSE CONCAT(SUBSTR(SPLIT_PART(email, '@', 1), 1, 2), '***@', SPLIT_PART(email, '@', 2))
    END AS email,
    CASE
        WHEN phone IS NULL OR LENGTH(regexp_replace(phone, '[^0-9]', '', 'g')) != 10 THEN 'XXXXXX0000'
        ELSE CONCAT('XXXXXX', RIGHT(regexp_replace(phone, '[^0-9]', '', 'g'), 4))
    END AS phone,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    CURRENT_TIMESTAMP AS staged_at
FROM deduplicated
WHERE rn = 1
  AND signup_date IS NOT NULL
  AND signup_date <= DATE '2026-09-30';

-- 2. Staging Products
CREATE OR REPLACE TABLE stg_products AS
WITH deduplicated AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        TRIM(unit) AS unit,
        TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        ROW_NUMBER() OVER (PARTITION BY TRIM(product_id) ORDER BY product_name ASC) AS rn
    FROM raw_products
)
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
    CURRENT_TIMESTAMP AS staged_at
FROM deduplicated
WHERE rn = 1
  AND category IS NOT NULL
  AND mrp > 0
  AND cost_price <= mrp;

-- 3. Staging Stores
CREATE OR REPLACE TABLE stg_stores AS
WITH deduplicated AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        TRIM(zone) AS zone,
        TRIM(area) AS area,
        TRY_CAST(pincode AS BIGINT) AS pincode,
        TRY_CAST(open_date AS DATE) AS open_date,
        TRIM(manager_name) AS manager_name,
        TRY_CAST(capacity_orders_per_day AS BIGINT) AS capacity_orders_per_day,
        ROW_NUMBER() OVER (PARTITION BY TRIM(store_id) ORDER BY store_name ASC) AS rn
    FROM raw_stores
)
SELECT
    store_id,
    store_name,
    zone,
    area,
    pincode,
    open_date,
    manager_name,
    capacity_orders_per_day,
    CURRENT_TIMESTAMP AS staged_at
FROM deduplicated
WHERE rn = 1
  AND zone IS NOT NULL
  AND capacity_orders_per_day > 0;

-- 4. Staging Orders
CREATE OR REPLACE TABLE stg_orders AS
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
    FROM raw_orders
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

-- 5. Staging Payments
CREATE OR REPLACE TABLE stg_payments AS
WITH deduplicated AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        UPPER(TRIM(payment_mode)) AS raw_pmode,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        ROW_NUMBER() OVER (PARTITION BY TRIM(payment_id) ORDER BY payment_ts ASC) AS rn
    FROM raw_payments
)
SELECT
    p.payment_id,
    p.order_id,
    CASE
        WHEN p.raw_pmode IN ('COD', 'CASH ON DELIVERY') THEN 'Cash on Delivery'
        WHEN p.raw_pmode IN ('UPI') THEN 'UPI'
        WHEN p.raw_pmode IN ('CREDIT CARD', 'CC') THEN 'Credit Card'
        WHEN p.raw_pmode IN ('DEBIT CARD', 'DC') THEN 'Debit Card'
        WHEN p.raw_pmode IN ('NET BANKING', 'NETBANKING') THEN 'Net Banking'
        ELSE 'Other'
    END AS payment_mode,
    p.amount_paid,
    p.payment_status,
    p.payment_ts,
    CURRENT_TIMESTAMP AS staged_at
FROM deduplicated p
WHERE p.rn = 1
  AND p.amount_paid > 0
  AND p.order_id IN (SELECT order_id FROM raw_orders);
