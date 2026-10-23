"""
scripts/develop_staging_sql.py
------------------------------
Develops and verifies staging SQL and compares September delivered revenue
between ETL and ELT.
"""

import os
import sys
import duckdb

sys.path.insert(0, os.path.abspath('include'))
import nammamart_helpers as helpers

def run():
    con = helpers.get_db_connection()

    sql = """
    -- 1. Staging Customers
    CREATE OR REPLACE TABLE stg_customers AS
    WITH ranked AS (
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
                ELSE 'Bronze'
            END AS loyalty_tier,
            TRY_CAST(is_active AS BOOLEAN) AS is_active,
            ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY signup_date ASC) AS rn
        FROM raw_customers
    )
    SELECT
        customer_id,
        full_name,
        CASE
            WHEN email IS NULL OR NOT regexp_matches(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN '***@masked.invalid'
            ELSE CONCAT(SUBSTR(SPLIT_PART(email, '@', 1), 1, 2), '***@', SPLIT_PART(email, '@', 2))
        END AS email,
        CASE
            WHEN phone IS NULL OR LENGTH(regexp_replace(phone, '[^0-9]', '', 'g')) != 10 THEN '******'
            ELSE CONCAT(SUBSTR(regexp_replace(phone, '[^0-9]', '', 'g'), 1, 2), '******', RIGHT(regexp_replace(phone, '[^0-9]', '', 'g'), 2))
        END AS phone,
        city,
        area,
        pincode,
        signup_date,
        loyalty_tier,
        is_active,
        CASE
            WHEN rn > 1 THEN 'FAILED'
            WHEN signup_date > DATE '2026-09-30' THEN 'FAILED'
            ELSE 'PASSED'
        END AS dq_status,
        CASE
            WHEN rn > 1 THEN 'Duplicate customer_id'
            WHEN signup_date > DATE '2026-09-30' THEN 'Future signup_date'
            ELSE NULL
        END AS failure_reason,
        CURRENT_TIMESTAMP AS staged_at
    FROM ranked;

    -- 2. Staging Products
    CREATE OR REPLACE TABLE stg_products AS
    WITH ranked AS (
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
        CASE
            WHEN rn > 1 THEN 'FAILED'
            WHEN category IS NULL OR category = '' THEN 'FAILED'
            WHEN mrp <= 0 THEN 'FAILED'
            WHEN cost_price > mrp THEN 'FAILED'
            ELSE 'PASSED'
        END AS dq_status,
        CASE
            WHEN rn > 1 THEN 'Duplicate product_id'
            WHEN category IS NULL OR category = '' THEN 'Missing category'
            WHEN mrp <= 0 THEN 'Non-positive MRP'
            WHEN cost_price > mrp THEN 'Cost price exceeds MRP'
            ELSE NULL
        END AS failure_reason,
        CURRENT_TIMESTAMP AS staged_at
    FROM ranked;

    -- 3. Staging Stores
    CREATE OR REPLACE TABLE stg_stores AS
    WITH ranked AS (
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
        CASE
            WHEN rn > 1 THEN 'FAILED'
            WHEN zone IS NULL OR zone = '' THEN 'FAILED'
            WHEN capacity_orders_per_day <= 0 THEN 'FAILED'
            ELSE 'PASSED'
        END AS dq_status,
        CASE
            WHEN rn > 1 THEN 'Duplicate store_id'
            WHEN zone IS NULL OR zone = '' THEN 'Missing zone'
            WHEN capacity_orders_per_day <= 0 THEN 'Non-positive capacity'
            ELSE NULL
        END AS failure_reason,
        CURRENT_TIMESTAMP AS staged_at
    FROM ranked;

    -- 4. Staging Payments
    CREATE OR REPLACE TABLE stg_payments AS
    WITH ranked AS (
        SELECT
            TRIM(payment_id) AS payment_id,
            TRIM(order_id) AS order_id,
            UPPER(TRIM(payment_mode)) AS raw_pmode,
            TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
            UPPER(TRIM(payment_status)) AS payment_status,
            TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
            TRIM(gateway_ref) AS gateway_ref,
            ROW_NUMBER() OVER (PARTITION BY TRIM(payment_id) ORDER BY payment_ts ASC) AS rn
        FROM raw_payments
    )
    SELECT
        p.payment_id,
        p.order_id,
        CASE
            WHEN p.raw_pmode IN ('COD', 'CASH ON DELIVERY', 'CASHH') THEN 'Cash on Delivery'
            WHEN p.raw_pmode IN ('UPI') THEN 'UPI'
            WHEN p.raw_pmode IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN p.raw_pmode IN ('WALLET') THEN 'Wallet'
            ELSE 'Other'
        END AS payment_mode,
        p.amount_paid,
        p.payment_status,
        p.payment_ts,
        p.gateway_ref,
        CASE
            WHEN p.rn > 1 THEN 'FAILED'
            WHEN p.amount_paid <= 0 THEN 'FAILED'
            WHEN p.order_id NOT IN (SELECT TRIM(order_id) FROM raw_orders) THEN 'FAILED'
            ELSE 'PASSED'
        END AS dq_status,
        CASE
            WHEN p.rn > 1 THEN 'Duplicate payment_id'
            WHEN p.amount_paid <= 0 THEN 'Non-positive amount_paid'
            WHEN p.order_id NOT IN (SELECT TRIM(order_id) FROM raw_orders) THEN 'Order ID not found in orders'
            ELSE NULL
        END AS failure_reason,
        CURRENT_TIMESTAMP AS staged_at
    FROM ranked p;

    -- 5. Staging Orders
    CREATE OR REPLACE TABLE stg_orders AS
    WITH parsed AS (
        SELECT
            TRIM(order_id) AS order_id,
            CASE
                WHEN order_ts LIKE '%31/09%' THEN NULL
                ELSE TRY_CAST(order_ts AS TIMESTAMP)
            END AS order_ts,
            TRIM(customer_id) AS customer_id,
            TRIM(store_id) AS store_id,
            TRIM(product_id) AS product_id,
            TRY_CAST(quantity AS BIGINT) AS quantity,
            TRY_CAST(unit_price AS DOUBLE) AS unit_price,
            TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
            TRY_CAST(order_amount AS DOUBLE) AS order_amount,
            CASE
                WHEN UPPER(TRIM(payment_mode)) IN ('COD', 'CASH ON DELIVERY', 'CASHH') THEN 'Cash on Delivery'
                WHEN UPPER(TRIM(payment_mode)) IN ('UPI') THEN 'UPI'
                WHEN UPPER(TRIM(payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
                WHEN UPPER(TRIM(payment_mode)) IN ('WALLET') THEN 'Wallet'
                ELSE 'Other'
            END AS payment_mode,
            CASE
                WHEN UPPER(TRIM(order_status)) LIKE '%DELIVERED%' THEN 'DELIVERED'
                WHEN UPPER(TRIM(order_status)) LIKE '%CANCELLED%' THEN 'CANCELLED'
                WHEN UPPER(TRIM(order_status)) LIKE '%RETURNED%' THEN 'RETURNED'
                ELSE UPPER(TRIM(order_status))
            END AS order_status,
            TRIM(channel) AS channel,
            TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
            ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY order_ts ASC) AS rn
        FROM raw_orders
    ),
    validated AS (
        SELECT
            o.*,
            p.cost_price,
            CASE
                WHEN o.rn > 1 THEN 'Duplicate order_id'
                WHEN o.order_ts IS NULL OR CAST(o.order_ts AS DATE) > DATE '2026-10-02' THEN 'Invalid or unparseable order_ts'
                WHEN o.customer_id IS NULL OR o.customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status = 'PASSED') THEN 'Invalid or missing customer_id'
                WHEN o.product_id IS NULL OR o.product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status = 'PASSED') THEN 'Invalid or missing product_id'
                WHEN o.store_id IS NULL OR o.store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status = 'PASSED') THEN 'Invalid or missing store_id'
                WHEN o.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'Invalid order_status'
                WHEN o.quantity <= 0 THEN 'Quantity <= 0'
                WHEN o.discount_pct < 0 OR o.discount_pct > 100 THEN 'Discount not 0-100'
                WHEN o.order_status = 'DELIVERED' AND (o.delivery_minutes IS NULL OR o.delivery_minutes < 0 OR o.delivery_minutes > 120) THEN 'Invalid delivery_minutes for DELIVERED'
                WHEN ABS(o.order_amount - ROUND(o.quantity * o.unit_price * (1.0 - o.discount_pct / 100.0), 2)) > 0.05 THEN 'Order amount does not match formula'
                ELSE NULL
            END AS failure_reason
        FROM parsed o
        LEFT JOIN (SELECT product_id, cost_price FROM stg_products WHERE dq_status = 'PASSED') p ON o.product_id = p.product_id
    )
    SELECT
        order_id,
        order_ts,
        CAST(order_ts AS DATE) AS order_date,
        customer_id,
        store_id,
        product_id,
        quantity,
        unit_price,
        discount_pct,
        order_amount,
        payment_mode,
        order_status,
        channel,
        delivery_minutes,
        cost_price,
        CASE
            WHEN failure_reason IS NULL AND order_status = 'DELIVERED' THEN ROUND(order_amount, 2)
            ELSE 0.0
        END AS net_revenue,
        CASE
            WHEN failure_reason IS NULL AND order_status = 'DELIVERED' THEN ROUND(order_amount - (quantity * cost_price), 2)
            ELSE 0.0
        END AS gross_margin,
        CASE
            WHEN failure_reason IS NULL AND order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN true
            ELSE false
        END AS within_15_min,
        CASE
            WHEN failure_reason IS NULL THEN 'PASSED'
            ELSE 'FAILED'
        END AS dq_status,
        failure_reason,
        CURRENT_TIMESTAMP AS staged_at
    FROM validated;
    """

    for stmt in sql.split(';'):
        if stmt.strip():
            con.execute(stmt)

    print("Staging tables created successfully!")
    passed_orders = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'PASSED'").fetchone()[0]
    failed_orders = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'FAILED'").fetchone()[0]
    print(f"stg_orders: {passed_orders} passed | {failed_orders} failed")

    elt_sept_rev = con.execute("""
        SELECT ROUND(SUM(net_revenue), 2)
        FROM stg_orders
        WHERE dq_status = 'PASSED'
          AND order_status = 'DELIVERED'
          AND order_date >= '2026-09-01' AND order_date <= '2026-09-30'
    """).fetchone()[0]
    print(f"ELT September Net Revenue: Rs. {elt_sept_rev:,.2f}")

    etl_sept_rev = con.execute("""
        SELECT ROUND(SUM(net_revenue), 2)
        FROM etl_fact_orders
        WHERE order_status = 'DELIVERED'
          AND order_date >= '2026-09-01' AND order_date <= '2026-09-30'
    """).fetchone()[0]
    print(f"ETL September Net Revenue: Rs. {etl_sept_rev:,.2f}")
    print(f"Difference: Rs. {abs(elt_sept_rev - etl_sept_rev):.2f}")

    con.close()

if __name__ == '__main__':
    run()
