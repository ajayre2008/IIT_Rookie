import sys, os, traceback
if sys.platform == 'win32':
    import concurrent.futures.thread, concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

sys.path.insert(0, os.path.abspath('include'))
sys.path.insert(0, os.path.abspath('dags'))
import nammamart_etl_ajay as etl
import nammamart_helpers as helpers

try:
    tasks = etl.nammamart_etl_dag.task_dict

    # 1. Extracts
    o_json = tasks['extract_orders'].python_callable()
    p_json = tasks['extract_payments'].python_callable()
    c_json = tasks['extract_customers'].python_callable()
    pr_json = tasks['extract_products'].python_callable()
    s_json = tasks['extract_stores'].python_callable()

    # 2. Validate
    val = tasks['validate_and_quarantine'].python_callable(
        orders_json=o_json, payments_json=p_json, customers_json=c_json,
        products_json=pr_json, stores_json=s_json, run_id='etl_manual_test'
    )
    print("\n--- VALIDATION PASSED ---")

    # 3. Transform
    tr = tasks['transform'].python_callable(val, run_id='etl_manual_test')
    print("--- TRANSFORM PASSED ---")

    # 4. Load
    ld = tasks['load_star_schema'].python_callable(tr)
    print("--- LOAD PASSED ---")

    # 5. Reconcile
    rec = tasks['reconcile_payments'].python_callable(ld, run_id='etl_manual_test')
    print("--- RECONCILE PASSED ---")

    # 6. Reports
    rep = tasks['generate_reports'].python_callable()
    print("--- REPORTS PASSED ---")

    # 7. Audit
    tasks['audit_pipeline_run'].python_callable(rec, rep, run_id='etl_manual_test')
    print("--- AUDIT PASSED ---")

    con = helpers.get_db_connection()
    fact_cnt = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    deliv_rev = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders WHERE order_status = 'DELIVERED'").fetchone()[0]
    print(f"\nETL Fact count: {fact_cnt} | Delivered Net Revenue: Rs. {deliv_rev:,.2f}")
    con.close()
except Exception as e:
    traceback.print_exc()
