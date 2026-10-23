-- include/sql/marts.sql
-- NammaMart Dimensional Marts Layer

CREATE OR REPLACE TABLE mart_daily_zone_revenue AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    COUNT(o.order_id) AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS daily_net_revenue
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, o.store_id, s.store_name
ORDER BY o.order_date, s.zone, o.store_id;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity) AS total_units_sold,
    ROUND(SUM(o.net_revenue), 2) AS category_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS total_cost,
    ROUND(SUM(o.gross_margin), 2) AS gross_margin,
    ROUND((SUM(o.gross_margin) / NULLIF(SUM(o.net_revenue), 0)) * 100, 2) AS margin_pct
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY category_revenue DESC;

CREATE OR REPLACE TABLE mart_reconciliation_summary AS
SELECT
    o.order_status,
    COALESCE(p.payment_status, 'NO_PAYMENT') AS payment_status,
    COUNT(*) AS order_count,
    ROUND(SUM(o.order_amount), 2) AS total_order_amount,
    ROUND(SUM(COALESCE(p.amount_paid, 0)), 2) AS total_paid_amount,
    ROUND(SUM(o.order_amount - COALESCE(p.amount_paid, 0)), 2) AS discrepancy_amount
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id
GROUP BY o.order_status, COALESCE(p.payment_status, 'NO_PAYMENT')
ORDER BY order_count DESC;

-- 3. Delivery SLA Compliance by Store
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(*) AS total_delivered_orders,
    ROUND(SUM(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_under_15_min,
    ROUND(SUM(CASE WHEN o.delivery_minutes > 15 AND o.delivery_minutes <= 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_15_to_20_min,
    ROUND(SUM(CASE WHEN o.delivery_minutes > 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_over_20_min,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY pct_under_15_min DESC;

-- 5. Top Customers by Total Spend & Loyalty
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.loyalty_tier,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_spend,
    ROUND(AVG(o.net_revenue), 2) AS avg_order_value
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.loyalty_tier
ORDER BY total_spend DESC;

-- 6. Store Capacity and Daily Load Analysis
CREATE OR REPLACE TABLE mart_store_capacity_check AS
SELECT
    o.order_date,
    s.store_id,
    s.store_name,
    s.capacity_orders_per_day,
    COUNT(o.order_id) AS actual_orders,
    COUNT(o.order_id) - s.capacity_orders_per_day AS overflow_count,
    CASE WHEN COUNT(o.order_id) > s.capacity_orders_per_day THEN 'OVER_CAPACITY' ELSE 'NORMAL' END AS capacity_status
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
GROUP BY o.order_date, s.store_id, s.store_name, s.capacity_orders_per_day
ORDER BY overflow_count DESC;
