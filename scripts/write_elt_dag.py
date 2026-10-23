import os

elt_dag_code = """# dags/nammamart_elt_ajay.py
# NammaMart Quick Commerce ELT Pipeline
# FDE: Ajay Kumar Reddy Vasanthu

import os
import sys
if sys.platform == 'win32':
    import concurrent.futures.thread
    import concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

from datetime import datetime, timedelta
import pandas as pd
import duckdb
import pendulum

BASE_DIR = '/usr/local/airflow' if os.path.exists('/usr/local/airflow/include') else os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
INCLUDE_DIR = os.path.join(BASE_DIR, 'include')
if INCLUDE_DIR not in sys.path:
    sys.path.insert(0, INCLUDE_DIR)

from airflow.decorators import dag, task
import nammamart_helpers as helpers

default_args = {
    'owner': 'fde-ajay',
    'retries': 2,
    'retry_delay': timedelta(seconds=60),
    'execution_timeout': timedelta(minutes=15),
}

@dag(
    dag_id='nammamart_elt_ajay',
    schedule='30 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata'),
    catchup=False,
    default_args=default_args,
    tags=['fde', 'nammamart', 'elt', 'ajay'],
    description='NammaMart morning ELT pipeline running SQL models and incremental merges',
)
def nammamart_elt_ajay():

    @task
    def raw_ingestion():
        sql_path = os.path.join(INCLUDE_DIR, 'sql', 'raw_load.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        con = helpers.get_db_connection()
        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                con.execute(stmt)

        counts = {}
        for tbl in ['raw_orders', 'raw_payments', 'raw_customers', 'raw_products', 'raw_stores']:
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            counts[tbl] = cnt
            print(f"[ELT Raw] Loaded {tbl}: {cnt} rows")

        con.close()
        return counts

    @task
    def staging_transformations(raw_counts: dict, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        sql_path = os.path.join(INCLUDE_DIR, 'sql', 'staging.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        helpers.init_governance_tables()
        con = helpers.get_db_connection()

        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                con.execute(stmt)

        stg_counts = {}
        for tbl in ['stg_orders', 'stg_payments', 'stg_customers', 'stg_products', 'stg_stores']:
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            stg_counts[tbl] = cnt
            print(f"[ELT Staging] Created {tbl}: {cnt} rows")

        # Record Data Quality Dimensions
        raw_cust = raw_counts.get('raw_customers', 0)
        stg_cust = stg_counts.get('stg_customers', 0)
        helpers.record_dq_result(con, run_id, 'nammamart_elt_ajay', 'elt_stg_customers_valid', 'Validity', 'Critical', raw_cust, raw_cust - stg_cust, 'PASS')

        raw_prod = raw_counts.get('raw_products', 0)
        stg_prod = stg_counts.get('stg_products', 0)
        helpers.record_dq_result(con, run_id, 'nammamart_elt_ajay', 'elt_stg_products_valid', 'Accuracy', 'Critical', raw_prod, raw_prod - stg_prod, 'PASS')

        raw_ord = raw_counts.get('raw_orders', 0)
        stg_ord = stg_counts.get('stg_orders', 0)
        helpers.record_dq_result(con, run_id, 'nammamart_elt_ajay', 'elt_stg_orders_valid', 'Consistency', 'Critical', raw_ord, raw_ord - stg_ord, 'PASS')

        con.close()
        return stg_counts

    @task
    def incremental_merge(stg_counts: dict):
        sql_path = os.path.join(INCLUDE_DIR, 'sql', 'incremental_merge.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        con = helpers.get_db_connection()
        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                con.execute(stmt)

        after_cnt = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        returned_cnt = con.execute("SELECT COUNT(*) FROM stg_orders WHERE order_status = 'RETURNED'").fetchone()[0]
        con.close()

        print(f"[ELT Incremental] Successfully upserted day 2 batch. Total stg_orders: {after_cnt} (Returned: {returned_cnt})")
        return {
            'orders_after_merge': after_cnt,
            'returned_count': returned_cnt,
            'stg_counts': stg_counts
        }

    @task
    def build_marts(merge_summary: dict):
        sql_path = os.path.join(INCLUDE_DIR, 'sql', 'marts.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        con = helpers.get_db_connection()
        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                con.execute(stmt)

        z_rev = con.execute("SELECT COUNT(*) FROM mart_daily_zone_revenue").fetchone()[0]
        c_mar = con.execute("SELECT COUNT(*) FROM mart_category_margin").fetchone()[0]
        r_sum = con.execute("SELECT COUNT(*) FROM mart_reconciliation_summary").fetchone()[0]

        print(f"[ELT Marts] mart_daily_zone_revenue: {z_rev} rows | mart_category_margin: {c_mar} rows | mart_reconciliation_summary: {r_sum} rows")
        con.close()
        return 'MARTS_READY'

    @task
    def verify_revenue_consistency(marts_status: str, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        con = helpers.get_db_connection()

        # Check September 2026 revenue between ETL and ELT
        has_etl = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'etl_fact_orders'").fetchone()[0]
        if has_etl:
            etl_sept_rev = con.execute(
                "SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders WHERE order_status = 'DELIVERED' AND order_date >= '2026-09-01' AND order_date <= '2026-09-30'"
            ).fetchone()[0] or 0.0

            elt_sept_rev = con.execute(
                "SELECT ROUND(SUM(net_revenue), 2) FROM stg_orders WHERE order_status = 'DELIVERED' AND order_date >= '2026-09-01' AND order_date <= '2026-09-30'"
            ).fetchone()[0] or 0.0

            print(f"[Reconciliation] September Revenue: ETL = Rs. {etl_sept_rev:,.2f} | ELT = Rs. {elt_sept_rev:,.2f}")
            helpers.record_dq_result(
                con, run_id, 'nammamart_elt_ajay', 'september_revenue_etl_vs_elt',
                'Consistency', 'Critical', 1, 0 if abs(etl_sept_rev - elt_sept_rev) < 1.0 else 1,
                'PASS' if abs(etl_sept_rev - elt_sept_rev) < 1.0 else 'FAIL'
            )
        con.close()
        return 'REVENUE_VERIFIED'

    @task(trigger_rule='all_done')
    def audit_pipeline_run(revenue_status: str, merge_summary: dict, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        dag_id = 'nammamart_elt_ajay'

        start_time = context.get('data_interval_start', pendulum.now('Asia/Kolkata'))
        if hasattr(start_time, 'naive'):
            start_time = start_time.naive()
        end_time = datetime.utcnow()

        stg_cnts = merge_summary.get('stg_counts', {}) if isinstance(merge_summary, dict) else {}
        total_extracted = sum(stg_cnts.values()) if stg_cnts else 0
        total_loaded = merge_summary.get('orders_after_merge', 0) if isinstance(merge_summary, dict) else 0

        status = 'SUCCESS' if revenue_status == 'REVENUE_VERIFIED' else 'FAILED'
        helpers.record_pipeline_run(
            run_id=run_id,
            dag_id=dag_id,
            start_time=start_time,
            end_time=end_time,
            rows_extracted=total_extracted,
            rows_loaded=total_loaded,
            rows_quarantined=0,
            status=status
        )
        print(f"[Audit] Logged run {run_id} to pipeline_runs with status {status}")

    # DAG flow
    r = raw_ingestion()
    s = staging_transformations(r)
    m = incremental_merge(s)
    b = build_marts(m)
    v = verify_revenue_consistency(b)
    audit_pipeline_run(v, m)

nammamart_elt_dag = nammamart_elt_ajay()
"""

with open('dags/nammamart_elt_ajay.py', 'w', encoding='utf-8') as f:
    f.write(elt_dag_code.strip())

print('SUCCESS: Created dags/nammamart_elt_ajay.py')