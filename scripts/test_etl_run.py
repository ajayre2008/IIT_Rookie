"""
scripts/test_etl_run.py
-----------------------
Runs the ETL logic from dags/nammamart_etl_ajay.py locally
and inspects the tables created in DuckDB.
"""

import os
import sys
if sys.platform == 'win32':
    import concurrent.futures.thread
    import concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

import pendulum
import duckdb
import pandas as pd

sys.path.insert(0, os.path.abspath('include'))
sys.path.insert(0, os.path.abspath('dags'))
import nammamart_helpers as helpers

def run():
    print("Testing ETL logic directly...")
    import nammamart_etl_ajay as etl

    # Execute extract tasks
    orders_json = etl.nammamart_etl_ajay.extract_orders.function()
    payments_json = etl.nammamart_etl_ajay.extract_payments.function()
    customers_json = etl.nammamart_etl_ajay.extract_customers.function()
    products_json = etl.nammamart_etl_ajay.extract_products.function()
    stores_json = etl.nammamart_etl_ajay.extract_stores.function()

    # Validate
    val = etl.nammamart_etl_ajay.validate_and_quarantine.function(
        orders_json, payments_json, customers_json, products_json, stores_json,
        run_id='test_etl_manual_1'
    )
    print("Validation result summary:", {k: v for k, v in val.items() if not k.endswith('_json') and not k.startswith('clean_')})

    # Transform
    trans = etl.nammamart_etl_ajay.transform.function(val, run_id='test_etl_manual_1')

    # Load
    load = etl.nammamart_etl_ajay.load_star_schema.function(trans)
    print("Load summary:", {k: v for k, v in load.items() if k != 'clean_payments'})

    # Reconcile
    rec = etl.nammamart_etl_ajay.reconcile_payments.function(load, run_id='test_etl_manual_1')
    print("Reconcile summary:", rec)

    # Reports
    rep = etl.nammamart_etl_ajay.generate_reports.function()
    print("Report status:", rep)

    # Audit
    etl.nammamart_etl_ajay.audit_pipeline_run.function(rec, rep, run_id='test_etl_manual_1')
    print("Audit logged successfully!")

    # Verify DuckDB contents
    con = helpers.get_db_connection()
    tables = con.execute("SHOW TABLES").fetchdf()
    print("\nDuckDB Tables:\n", tables)

    fact_cnt = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    deliv_rev = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders WHERE order_status = 'DELIVERED'").fetchone()[0]
    print(f"\nETL Fact Orders: {fact_cnt} rows | Delivered Net Revenue: Rs. {deliv_rev:,.2f}")

    dq_cnt = con.execute("SELECT COUNT(*) FROM dq_results").fetchone()[0]
    print(f"DQ Results: {dq_cnt} check rows recorded")
    runs_cnt = con.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
    print(f"Pipeline Runs: {runs_cnt} audit rows recorded")
    recon_cnt = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
    print(f"ETL Payment Reconciliation: {recon_cnt} mismatch rows recorded")

    con.close()

if __name__ == '__main__':
    run()
