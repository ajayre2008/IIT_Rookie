import sys, os, traceback
if sys.platform == 'win32':
    import concurrent.futures.thread, concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

sys.path.insert(0, os.path.abspath('include'))
sys.path.insert(0, os.path.abspath('dags'))
import nammamart_elt_ajay as elt
import nammamart_helpers as helpers

try:
    tasks = elt.nammamart_elt_dag.task_dict

    # 1. Raw Ingestion
    print("--- RUNNING RAW INGESTION ---")
    r_counts = tasks['raw_ingestion'].python_callable()
    print("Raw counts:", r_counts)

    # 2. Staging Transformations
    print("\n--- RUNNING STAGING TRANSFORMATIONS ---")
    s_counts = tasks['staging_transformations'].python_callable(raw_counts=r_counts, run_id='elt_manual_test')
    print("Staging counts:", s_counts)

    # 3. Incremental Merge
    print("\n--- RUNNING INCREMENTAL MERGE ---")
    m_summary = tasks['incremental_merge'].python_callable(stg_counts=s_counts)
    print("Merge summary:", m_summary)

    # 4. Build Marts
    print("\n--- RUNNING BUILD MARTS ---")
    b_status = tasks['build_marts'].python_callable(merge_summary=m_summary)
    print("Marts status:", b_status)

    # 5. Verify Revenue Consistency
    print("\n--- RUNNING REVENUE VERIFICATION ---")
    v_status = tasks['verify_revenue_consistency'].python_callable(marts_status=b_status, run_id='elt_manual_test')
    print("Revenue status:", v_status)

    # 6. Audit Pipeline Run
    print("\n--- RUNNING AUDIT PIPELINE RUN ---")
    tasks['audit_pipeline_run'].python_callable(revenue_status=v_status, merge_summary=m_summary, run_id='elt_manual_test')
    print("ELT Audit logged successfully!")

    # Verify Marts queries
    con = helpers.get_db_connection()
    print("\n=== MART ROW COUNTS ===")
    for m in ['mart_daily_zone_revenue', 'mart_category_margin', 'mart_delivery_sla', 'mart_reconciliation_summary', 'mart_top_customers', 'dq_results', 'pipeline_runs']:
        cnt = con.execute(f"SELECT COUNT(*) FROM {m}").fetchone()[0]
        print(f"  {m}: {cnt} rows")

    print("\n=== TOP 5 STORES BY SEPTEMBER REVENUE ===")
    df_stores = con.execute("""
        SELECT store_name, zone, ROUND(SUM(daily_net_revenue), 2) as total_rev
        FROM mart_daily_zone_revenue
        WHERE order_date >= '2026-09-01' AND order_date <= '2026-09-30'
        GROUP BY store_name, zone
        ORDER BY total_rev DESC
        LIMIT 5
    """).df()
    print(df_stores.to_string())

    print("\n=== OVERALL SEPTEMBER DELIVERED REVENUE IN MARTS ===")
    sept_mart_rev = con.execute("""
        SELECT ROUND(SUM(daily_net_revenue), 2)
        FROM mart_daily_zone_revenue
        WHERE order_date >= '2026-09-01' AND order_date <= '2026-09-30'
    """).fetchone()[0]
    print(f"September Total Delivered Mart Revenue: Rs. {sept_mart_rev:,.2f}")

    con.close()

except Exception as e:
    traceback.print_exc()
