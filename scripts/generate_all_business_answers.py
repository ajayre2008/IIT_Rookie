import sys, os
if sys.platform == 'win32':
    import concurrent.futures.thread, concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

sys.path.insert(0, 'include')
import nammamart_helpers as helpers
con = helpers.get_db_connection()

print("="*60)
print("NAMMAMART BUSINESS INTELLIGENCE & RECONCILIATION REPORT")
print("="*60)

# Q1: Top 3 and Bottom 3 Stores by September Net Delivered Revenue
print("\n--- QUESTION 1: TOP 3 & BOTTOM 3 STORES (SEPTEMBER NET DELIVERED REVENUE) ---")
q1_top = con.execute("""
    SELECT store_id, store_name, zone, ROUND(SUM(daily_net_revenue), 2) AS september_revenue
    FROM mart_daily_zone_revenue
    WHERE order_date >= '2026-09-01' AND order_date <= '2026-09-30'
    GROUP BY store_id, store_name, zone
    ORDER BY september_revenue DESC
    LIMIT 3
""").df()
print("Top 3 Stores:")
print(q1_top.to_string(index=False))

q1_bottom = con.execute("""
    SELECT store_id, store_name, zone, ROUND(SUM(daily_net_revenue), 2) AS september_revenue
    FROM mart_daily_zone_revenue
    WHERE order_date >= '2026-09-01' AND order_date <= '2026-09-30'
    GROUP BY store_id, store_name, zone
    ORDER BY september_revenue ASC
    LIMIT 3
""").df()
print("\nBottom 3 Stores:")
print(q1_bottom.to_string(index=False))

# Q2: Category Gross Margin Breakdown
print("\n--- QUESTION 2: CATEGORY GROSS MARGIN BREAKDOWN ---")
q2 = con.execute("""
    SELECT category, total_orders, total_units_sold, category_revenue, total_cost, gross_margin, margin_pct
    FROM mart_category_margin
    ORDER BY margin_pct DESC
""").df()
print(q2.to_string(index=False))

# Q3: Delivery SLA Compliance by Store
print("\n--- QUESTION 3: DELIVERY SLA COMPLIANCE BY STORE ---")
q3 = con.execute("""
    SELECT store_id, store_name, zone, total_delivered_orders, pct_under_15_min, pct_15_to_20_min, pct_over_20_min, avg_delivery_minutes,
           CASE WHEN pct_under_15_min < 85.0 THEN 'FAILED (<85%)' ELSE 'MET (>=85%)' END AS sla_target_status
    FROM mart_delivery_sla
    ORDER BY pct_under_15_min ASC
""").df()
print(q3.to_string(index=False))

# Q4: Payment Reconciliation
print("\n--- QUESTION 4: PAYMENT RECONCILIATION ANOMALIES ---")
q4_summary = con.execute("""
    SELECT order_status, payment_status, order_count, total_order_amount, total_paid_amount, discrepancy_amount
    FROM mart_reconciliation_summary
""").df()
print("Reconciliation Summary Mart:")
print(q4_summary.to_string(index=False))

q4_a = con.execute("""
    SELECT COUNT(*) AS mismatch_count, ROUND(SUM(ABS(o.order_amount - p.amount_paid)), 2) AS total_amount_mismatch
    FROM stg_orders o
    JOIN stg_payments p ON o.order_id = p.order_id
    WHERE ABS(o.order_amount - p.amount_paid) > 0.01
""").df()
print("\n4a. Order Amount vs Paid Amount Mismatch:")
print(q4_a.to_string(index=False))

q4_b = con.execute("""
    SELECT COUNT(*) AS delivered_failed_count, ROUND(SUM(o.order_amount), 2) AS at_risk_delivered_amount
    FROM stg_orders o
    JOIN stg_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'DELIVERED' AND p.payment_status = 'FAILED'
""").df()
print("\n4b. Orders DELIVERED but Payment FAILED:")
print(q4_b.to_string(index=False))

q4_c = con.execute("""
    SELECT COUNT(*) AS cancelled_success_count, ROUND(SUM(p.amount_paid), 2) AS refund_due_amount
    FROM stg_orders o
    JOIN stg_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS'
""").df()
print("\n4c. Orders CANCELLED but Payment SUCCESS:")
print(q4_c.to_string(index=False))

# Q5: Top 10 Customers by Total Spend
print("\n--- QUESTION 5: TOP 10 CUSTOMERS BY TOTAL SPEND & LOYALTY TIER ---")
q5 = con.execute("""
    SELECT customer_id, full_name, loyalty_tier, total_orders, total_spend, avg_order_value
    FROM mart_top_customers
    ORDER BY total_spend DESC
    LIMIT 10
""").df()
print(q5.to_string(index=False))

# Q6: Day 2 Incremental Impact
print("\n--- QUESTION 6: DAY 2 INCREMENTAL FEED IMPACT ---")
etl_sept = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders WHERE order_status = 'DELIVERED'").fetchone()[0]
elt_sept = con.execute("SELECT ROUND(SUM(daily_net_revenue), 2) FROM mart_daily_zone_revenue WHERE order_date >= '2026-09-01' AND order_date <= '2026-09-30'").fetchone()[0]
elt_oct1 = con.execute("SELECT ROUND(SUM(daily_net_revenue), 2) FROM mart_daily_zone_revenue WHERE order_date = '2026-10-01'").fetchone()[0]
elt_total = con.execute("SELECT ROUND(SUM(daily_net_revenue), 2) FROM mart_daily_zone_revenue").fetchone()[0]

print(f"a. September Revenue Before Day 2 feed (ETL Baseline): Rs. {etl_sept:,.2f}")
print(f"   September Revenue After Day 2 feed (Returns Deducted): Rs. {elt_sept:,.2f}")
print(f"   October 1st Delivered Revenue: Rs. {elt_oct1:,.2f}")
print(f"   Combined Total Delivered Revenue: Rs. {elt_total:,.2f}")

ret_count = con.execute("""
    SELECT COUNT(*)
    FROM stg_orders
    WHERE order_id IN (SELECT order_id FROM read_csv_auto('include/data/nammamart/orders_2026-10-01_increment.csv') WHERE order_status = 'RETURNED')
""").fetchone()[0]
print(f"b. Newly Returned September Orders: {ret_count}")
print(f"c. Revenue Impact of Returned Orders: -Rs. {etl_sept - elt_sept:,.2f}")

# Capacity check on Oct 1
print("\nd. Store Capacity Check on Oct 1, 2026:")
cap_oct1 = con.execute("""
    SELECT order_date, store_id, store_name, capacity_orders_per_day, actual_orders, overflow_count, capacity_status
    FROM mart_store_capacity_check
    WHERE order_date = '2026-10-01'
    ORDER BY actual_orders DESC
""").df()
print(cap_oct1.to_string(index=False))

# Overall pipeline DQ results summary
print("\n--- DATA QUALITY RESULTS SUMMARY ---")
dq_summary = con.execute("""
    SELECT dimension, severity, status, COUNT(*) AS check_count, SUM(rows_failed) AS total_failures
    FROM dq_results
    GROUP BY dimension, severity, status
    ORDER BY dimension, severity
""").df()
print(dq_summary.to_string(index=False))

con.close()
