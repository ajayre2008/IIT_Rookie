"""
scripts/test_elt_sql.py
-----------------------
Test and verify raw, staging, incremental merge, and marts SQL scripts.
Compares September revenue between ETL and ELT.
"""

import os
import sys
import duckdb
import pandas as pd

sys.path.insert(0, os.path.abspath('include'))
import nammamart_helpers as helpers

def test_sql():
    con = helpers.get_db_connection()

    # 1. Raw Load
    print("Testing raw_load.sql...")
    with open('include/sql/raw_load.sql', 'r', encoding='utf-8') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            con.execute(stmt)

    for tbl in ['raw_orders', 'raw_payments', 'raw_customers', 'raw_products', 'raw_stores']:
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {cnt} rows")

    # 2. Staging Load
    print("\nTesting staging.sql...")
    with open('include/sql/staging.sql', 'r', encoding='utf-8') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            con.execute(stmt)

    for tbl in ['stg_orders', 'stg_payments', 'stg_customers', 'stg_products', 'stg_stores']:
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        passed = con.execute(f"SELECT COUNT(*) FROM {tbl} WHERE dq_status = 'PASSED'").fetchone()[0] if 'orders' in tbl else cnt
        print(f"  {tbl}: {cnt} rows (passed: {passed})")

    # Check September revenue in stg_orders before incremental load
    elt_sept_rev = con.execute("""
        SELECT ROUND(SUM(net_revenue), 2)
        FROM stg_orders
        WHERE dq_status = 'PASSED'
          AND order_status = 'DELIVERED'
          AND order_date >= '2026-09-01' AND order_date <= '2026-09-30'
    """).fetchone()[0]
    print(f"\nELT September Delivered Net Revenue (pre-increment): Rs. {elt_sept_rev:,.2f}")

    # Check ETL September revenue if exists
    has_etl = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'etl_fact_orders'").fetchone()[0]
    if has_etl:
        etl_sept_rev = con.execute("""
            SELECT ROUND(SUM(net_revenue), 2)
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
              AND order_date >= '2026-09-01' AND order_date <= '2026-09-30'
        """).fetchone()[0]
        print(f"ETL September Delivered Net Revenue: Rs. {etl_sept_rev:,.2f}")
        diff = abs(elt_sept_rev - etl_sept_rev)
        print(f"Difference between ETL and ELT: Rs. {diff:.2f}")

    # 3. Incremental Merge
    print("\nTesting incremental_merge.sql...")
    with open('include/sql/incremental_merge.sql', 'r', encoding='utf-8') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            con.execute(stmt)

    stg_after = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    raw_after = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    print(f"  stg_orders after merge: {stg_after} rows")
    print(f"  raw_orders after merge: {raw_after} rows")

    # 4. Marts
    print("\nTesting marts.sql...")
    with open('include/sql/marts.sql', 'r', encoding='utf-8') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            con.execute(stmt)

    for tbl in ['mart_daily_store_kpis', 'mart_category_margin', 'mart_delivery_sla', 'mart_payment_reconciliation', 'mart_top_customers']:
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {cnt} rows")

    con.close()

if __name__ == '__main__':
    test_sql()
