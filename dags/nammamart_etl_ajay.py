# dags/nammamart_etl_ajay.py
# NammaMart Quick Commerce Morning ETL Pipeline
# FDE: Ajay Kumar Reddy Vasanthu

import os
import sys
if sys.platform == 'win32':
    import concurrent.futures.thread
    import concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

from datetime import datetime, timedelta
import io
import pandas as pd
import duckdb
import pendulum

# Ensure include directory is in sys.path
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
    dag_id='nammamart_etl_ajay',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata'),
    catchup=False,
    default_args=default_args,
    tags=['fde', 'nammamart', 'etl', 'ajay'],
    description='NammaMart morning ETL pipeline delivering reconciled revenue by 9:00 AM IST',
)
def nammamart_etl_ajay():

    @task
    def extract_orders():
        path = os.path.join(helpers.get_data_dir(), 'orders.csv')
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} orders from {path}")
        return df.to_json(orient='split', date_format='iso')

    @task
    def extract_payments():
        path = os.path.join(helpers.get_data_dir(), 'payments.csv')
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} payments from {path}")
        return df.to_json(orient='split', date_format='iso')

    @task
    def extract_customers():
        path = os.path.join(helpers.get_data_dir(), 'customers.csv')
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} customers from {path}")
        return df.to_json(orient='split', date_format='iso')

    @task
    def extract_products():
        path = os.path.join(helpers.get_data_dir(), 'products.csv')
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} products from {path}")
        return df.to_json(orient='split', date_format='iso')

    @task
    def extract_stores():
        path = os.path.join(helpers.get_data_dir(), 'stores.csv')
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} stores from {path}")
        return df.to_json(orient='split', date_format='iso')

    @task
    def validate_and_quarantine(orders_json: str, payments_json: str,
                                customers_json: str, products_json: str,
                                stores_json: str, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        dag_id = 'nammamart_etl_ajay'

        orders = pd.read_json(io.StringIO(orders_json), orient='split')
        payments = pd.read_json(io.StringIO(payments_json), orient='split')
        customers = pd.read_json(io.StringIO(customers_json), orient='split')
        products = pd.read_json(io.StringIO(products_json), orient='split')
        stores = pd.read_json(io.StringIO(stores_json), orient='split')

        helpers.init_governance_tables()
        con = helpers.get_db_connection()
        quarantine_dir = helpers.get_quarantine_dir()
        ts_str = pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')

        # 1. Validate Customers
        c_dupe = customers.duplicated(subset=['customer_id'], keep='first')
        c_future = pd.to_datetime(customers['signup_date'], errors='coerce') > pd.to_datetime('2026-09-30')
        c_bad_phone = customers['phone'].astype(str).str.replace(r'\D', '', regex=True).str.len() != 10
        c_bad_email = customers['email'].isna() | (~customers['email'].astype(str).str.contains('@'))

        helpers.record_dq_result(con, run_id, dag_id, 'customers_unique_id', 'Uniqueness', 'Critical', len(customers), int(c_dupe.sum()), 'FAIL' if c_dupe.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'customers_signup_timeliness', 'Timeliness', 'Critical', len(customers), int(c_future.sum()), 'FAIL' if c_future.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'customers_phone_10digits', 'Validity', 'Warning', len(customers), int(c_bad_phone.sum()), 'WARN' if c_bad_phone.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'customers_valid_email', 'Validity', 'Warning', len(customers), int(c_bad_email.sum()), 'WARN' if c_bad_email.sum() > 0 else 'PASS')

        c_critical = c_dupe | c_future
        if c_critical.sum() > 0:
            c_failed = customers[c_critical].copy()
            c_failed['failure_reason'] = [
                '; '.join([
                    *(['Duplicate customer_id'] if d else []),
                    *(['Future signup_date'] if f else []),
                ])
                for d, f in zip(c_dupe[c_critical], c_future[c_critical])
            ]
            c_failed.to_csv(os.path.join(quarantine_dir, f'customers_failed_{ts_str}.csv'), index=False)
        clean_customers = customers[~c_critical].copy()

        # 2. Validate Products
        p_dupe = products.duplicated(subset=['product_id'], keep='first')
        p_null_cat = products['category'].isna()
        p_bad_mrp = products['mrp'] <= 0
        p_cost_gt_mrp = products['cost_price'] > products['mrp']

        helpers.record_dq_result(con, run_id, dag_id, 'products_unique_id', 'Uniqueness', 'Critical', len(products), int(p_dupe.sum()), 'FAIL' if p_dupe.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'products_category_completeness', 'Completeness', 'Critical', len(products), int(p_null_cat.sum()), 'FAIL' if p_null_cat.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'products_positive_mrp', 'Accuracy', 'Critical', len(products), int(p_bad_mrp.sum()), 'FAIL' if p_bad_mrp.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'products_cost_le_mrp', 'Accuracy', 'Critical', len(products), int(p_cost_gt_mrp.sum()), 'FAIL' if p_cost_gt_mrp.sum() > 0 else 'PASS')

        p_critical = p_dupe | p_null_cat | p_bad_mrp | p_cost_gt_mrp
        if p_critical.sum() > 0:
            p_failed = products[p_critical].copy()
            p_failed['failure_reason'] = [
                '; '.join([
                    *(['Duplicate product_id'] if d else []),
                    *(['Missing category'] if c else []),
                    *(['Non-positive MRP'] if m else []),
                    *(['Cost price exceeds MRP'] if cm else []),
                ])
                for d, c, m, cm in zip(p_dupe[p_critical], p_null_cat[p_critical], p_bad_mrp[p_critical], p_cost_gt_mrp[p_critical])
            ]
            p_failed.to_csv(os.path.join(quarantine_dir, f'products_failed_{ts_str}.csv'), index=False)
        clean_products = products[~p_critical].copy()

        # 3. Validate Stores
        s_dupe = stores.duplicated(subset=['store_id'], keep='first')
        s_null_zone = stores['zone'].isna()
        s_zero_cap = stores['capacity_orders_per_day'] <= 0

        helpers.record_dq_result(con, run_id, dag_id, 'stores_unique_id', 'Uniqueness', 'Critical', len(stores), int(s_dupe.sum()), 'FAIL' if s_dupe.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'stores_zone_completeness', 'Completeness', 'Critical', len(stores), int(s_null_zone.sum()), 'FAIL' if s_null_zone.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'stores_positive_capacity', 'Accuracy', 'Critical', len(stores), int(s_zero_cap.sum()), 'FAIL' if s_zero_cap.sum() > 0 else 'PASS')

        s_critical = s_dupe | s_null_zone | s_zero_cap
        if s_critical.sum() > 0:
            s_failed = stores[s_critical].copy()
            s_failed['failure_reason'] = [
                '; '.join([
                    *(['Duplicate store_id'] if d else []),
                    *(['Missing zone'] if z else []),
                    *(['Non-positive capacity'] if c else []),
                ])
                for d, z, c in zip(s_dupe[s_critical], s_null_zone[s_critical], s_zero_cap[s_critical])
            ]
            s_failed.to_csv(os.path.join(quarantine_dir, f'stores_failed_{ts_str}.csv'), index=False)
        clean_stores = stores[~s_critical].copy()

        # 4. Validate Payments
        pay_dupe = payments.duplicated(subset=['payment_id'], keep='first')
        pay_neg = payments['amount_paid'] <= 0
        pay_bad_fk = ~payments['order_id'].isin(orders['order_id'])

        helpers.record_dq_result(con, run_id, dag_id, 'payments_unique_id', 'Uniqueness', 'Critical', len(payments), int(pay_dupe.sum()), 'FAIL' if pay_dupe.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'payments_positive_amount', 'Accuracy', 'Critical', len(payments), int(pay_neg.sum()), 'FAIL' if pay_neg.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'payments_order_fk', 'Consistency', 'Critical', len(payments), int(pay_bad_fk.sum()), 'FAIL' if pay_bad_fk.sum() > 0 else 'PASS')

        pay_critical = pay_dupe | pay_neg | pay_bad_fk
        if pay_critical.sum() > 0:
            pay_failed = payments[pay_critical].copy()
            pay_failed['failure_reason'] = [
                '; '.join([
                    *(['Duplicate payment_id'] if d else []),
                    *(['Non-positive amount_paid'] if n else []),
                    *(['Order ID not found in orders'] if f else []),
                ])
                for d, n, f in zip(pay_dupe[pay_critical], pay_neg[pay_critical], pay_bad_fk[pay_critical])
            ]
            pay_failed.to_csv(os.path.join(quarantine_dir, f'payments_failed_{ts_str}.csv'), index=False)
        clean_payments = payments[~pay_critical].copy()

        # 5. Validate Orders
        valid_cust_ids = set(clean_customers['customer_id'].astype(str).str.strip())
        valid_prod_ids = set(clean_products['product_id'].astype(str).str.strip())
        valid_store_ids = set(clean_stores['store_id'].astype(str).str.strip())
        valid_statuses = {'DELIVERED', 'CANCELLED', 'RETURNED'}

        o_dupe = orders.duplicated(subset=['order_id'], keep='first')
        o_fk_cust = ~orders['customer_id'].astype(str).str.strip().isin(valid_cust_ids)
        o_fk_prod = ~orders['product_id'].astype(str).str.strip().isin(valid_prod_ids)
        o_fk_store = ~orders['store_id'].astype(str).str.strip().isin(valid_store_ids)

        def check_ts(val):
            if pd.isna(val) or '31/09' in str(val):
                return False
            try:
                dt = pd.to_datetime(val)
                return dt <= pd.to_datetime('2026-10-02')
            except:
                return False

        o_bad_ts = ~orders['order_ts'].apply(check_ts)
        o_bad_status = ~orders['order_status'].astype(str).str.strip().str.upper().isin(valid_statuses)
        o_bad_qty = orders['quantity'] <= 0
        o_bad_disc = (orders['discount_pct'] < 0) | (orders['discount_pct'] > 100)

        status_clean = orders['order_status'].astype(str).str.strip().str.upper()
        o_bad_deliv = (status_clean == 'DELIVERED') & (
            orders['delivery_minutes'].isna() | (orders['delivery_minutes'] < 0) | (orders['delivery_minutes'] > 120)
        )

        expected_amt = (orders['quantity'] * orders['unit_price'] * (1 - orders['discount_pct'] / 100)).round(2)
        o_bad_amt = (orders['order_amount'] - expected_amt).abs() > 0.05

        helpers.record_dq_result(con, run_id, dag_id, 'orders_unique_order_id', 'Uniqueness', 'Critical', len(orders), int(o_dupe.sum()), 'FAIL' if o_dupe.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_customer_fk', 'Consistency', 'Critical', len(orders), int(o_fk_cust.sum()), 'FAIL' if o_fk_cust.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_product_fk', 'Consistency', 'Critical', len(orders), int(o_fk_prod.sum()), 'FAIL' if o_fk_prod.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_store_fk', 'Consistency', 'Critical', len(orders), int(o_fk_store.sum()), 'FAIL' if o_fk_store.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_valid_order_ts', 'Timeliness', 'Critical', len(orders), int(o_bad_ts.sum()), 'FAIL' if o_bad_ts.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_valid_status', 'Validity', 'Critical', len(orders), int(o_bad_status.sum()), 'FAIL' if o_bad_status.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_positive_quantity', 'Accuracy', 'Critical', len(orders), int(o_bad_qty.sum()), 'FAIL' if o_bad_qty.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_discount_range', 'Validity', 'Critical', len(orders), int(o_bad_disc.sum()), 'FAIL' if o_bad_disc.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_delivered_sla', 'Accuracy', 'Critical', len(orders), int(o_bad_deliv.sum()), 'FAIL' if o_bad_deliv.sum() > 0 else 'PASS')
        helpers.record_dq_result(con, run_id, dag_id, 'orders_amount_formula', 'Accuracy', 'Critical', len(orders), int(o_bad_amt.sum()), 'FAIL' if o_bad_amt.sum() > 0 else 'PASS')

        o_critical_fail = (
            o_dupe | o_fk_cust | o_fk_prod | o_fk_store | o_bad_ts |
            o_bad_status | o_bad_qty | o_bad_disc | o_bad_deliv | o_bad_amt
        )

        failed_orders_count = int(o_critical_fail.sum())
        passed_orders_count = len(orders) - failed_orders_count

        if failed_orders_count > 0:
            o_failed_df = orders[o_critical_fail].copy()
            reasons = []
            for idx, row in o_failed_df.iterrows():
                r = []
                if o_dupe.loc[idx]: r.append('Duplicate order_id')
                if o_fk_cust.loc[idx]: r.append('Invalid or missing customer_id')
                if o_fk_prod.loc[idx]: r.append('Invalid or missing product_id')
                if o_fk_store.loc[idx]: r.append('Invalid or missing store_id')
                if o_bad_ts.loc[idx]: r.append('Invalid or unparseable order_ts')
                if o_bad_status.loc[idx]: r.append(f"Invalid order_status ({row['order_status']})")
                if o_bad_qty.loc[idx]: r.append(f"Quantity <= 0 ({row['quantity']})")
                if o_bad_disc.loc[idx]: r.append(f"Discount not 0-100 ({row['discount_pct']})")
                if o_bad_deliv.loc[idx]: r.append(f"Invalid delivery_minutes for DELIVERED ({row['delivery_minutes']})")
                if o_bad_amt.loc[idx]: r.append('Order amount does not match formula')
                reasons.append('; '.join(r))
            o_failed_df['failure_reason'] = reasons
            quarantine_path = os.path.join(quarantine_dir, f'orders_failed_{ts_str}.csv')
            o_failed_df.to_csv(quarantine_path, index=False)
            print(f"[Quarantine] Wrote {failed_orders_count} rejected orders to {quarantine_path}")

        clean_orders = orders[~o_critical_fail].copy()
        con.close()

        print(f"[Validate] Orders: {len(orders)} total | {passed_orders_count} passed | {failed_orders_count} quarantined")

        total_extracted = len(orders) + len(payments) + len(customers) + len(products) + len(stores)
        total_quarantined = failed_orders_count + int(c_critical.sum()) + int(p_critical.sum()) + int(s_critical.sum()) + int(pay_critical.sum())

        return {
            'clean_orders': clean_orders.to_json(orient='split', date_format='iso'),
            'clean_customers': clean_customers.to_json(orient='split', date_format='iso'),
            'clean_products': clean_products.to_json(orient='split', date_format='iso'),
            'clean_stores': clean_stores.to_json(orient='split', date_format='iso'),
            'clean_payments': clean_payments.to_json(orient='split', date_format='iso'),
            'total_extracted': total_extracted,
            'total_quarantined': total_quarantined,
        }

    @task
    def transform(validated_data: dict, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        now_ts = pendulum.now('Asia/Kolkata').to_iso8601_string()

        orders = pd.read_json(io.StringIO(validated_data['clean_orders']), orient='split')
        customers = pd.read_json(io.StringIO(validated_data['clean_customers']), orient='split')
        products = pd.read_json(io.StringIO(validated_data['clean_products']), orient='split')
        stores = pd.read_json(io.StringIO(validated_data['clean_stores']), orient='split')

        # Standardise Customers
        customers['customer_id'] = customers['customer_id'].astype(str).str.strip()
        customers['full_name'] = customers['full_name'].astype(str).str.strip()
        customers['loyalty_tier'] = customers['loyalty_tier'].apply(helpers.standardize_loyalty_tier)
        customers['email_masked'] = customers['email'].apply(helpers.mask_email)
        customers['phone_masked'] = customers['phone'].apply(helpers.mask_phone)
        customers = customers.drop_duplicates(subset=['customer_id'], keep='first')
        customers['loaded_at'] = now_ts
        customers['run_id'] = run_id

        dim_customer = customers[[
            'customer_id', 'full_name', 'email_masked', 'phone_masked',
            'city', 'area', 'pincode', 'signup_date', 'loyalty_tier',
            'is_active', 'loaded_at', 'run_id'
        ]].rename(columns={'email_masked': 'email', 'phone_masked': 'phone'})

        # Standardise Products
        products['product_id'] = products['product_id'].astype(str).str.strip()
        products['product_name'] = products['product_name'].astype(str).str.strip()
        products['category'] = products['category'].astype(str).str.strip()
        products = products.drop_duplicates(subset=['product_id'], keep='first')
        products['loaded_at'] = now_ts
        products['run_id'] = run_id

        dim_product = products[[
            'product_id', 'product_name', 'category', 'sub_category',
            'brand', 'mrp', 'cost_price', 'unit', 'is_perishable',
            'launch_date', 'loaded_at', 'run_id'
        ]]

        # Standardise Stores
        stores['store_id'] = stores['store_id'].astype(str).str.strip()
        stores['store_name'] = stores['store_name'].astype(str).str.strip()
        stores['zone'] = stores['zone'].astype(str).str.strip()
        stores = stores.drop_duplicates(subset=['store_id'], keep='first')
        stores['loaded_at'] = now_ts
        stores['run_id'] = run_id

        dim_store = stores[[
            'store_id', 'store_name', 'zone', 'area', 'pincode',
            'open_date', 'manager_name', 'capacity_orders_per_day',
            'loaded_at', 'run_id'
        ]]

        # Standardise Orders
        orders['order_id'] = orders['order_id'].astype(str).str.strip()
        orders['customer_id'] = orders['customer_id'].astype(str).str.strip()
        orders['product_id'] = orders['product_id'].astype(str).str.strip()
        orders['store_id'] = orders['store_id'].astype(str).str.strip()
        orders['order_ts'] = pd.to_datetime(orders['order_ts'])
        orders['order_date'] = orders['order_ts'].dt.date.astype(str)
        orders['order_status'] = orders['order_status'].apply(helpers.standardize_order_status)
        orders['payment_mode'] = orders['payment_mode'].apply(helpers.standardize_payment_mode)

        orders_enriched = orders.merge(
            dim_product[['product_id', 'cost_price']], on='product_id', how='left'
        )

        orders_enriched['net_revenue'] = orders_enriched.apply(
            lambda r: round(float(r['order_amount']), 2) if r['order_status'] == 'DELIVERED' else 0.0,
            axis=1
        )
        orders_enriched['gross_margin'] = orders_enriched.apply(
            lambda r: round(float(r['net_revenue']) - (float(r['quantity']) * float(r['cost_price'])), 2)
            if r['order_status'] == 'DELIVERED' else 0.0,
            axis=1
        )
        orders_enriched['within_15_min'] = orders_enriched.apply(
            lambda r: bool(r['order_status'] == 'DELIVERED' and pd.notna(r['delivery_minutes']) and r['delivery_minutes'] <= 15),
            axis=1
        )

        orders_enriched['loaded_at'] = now_ts
        orders_enriched['run_id'] = run_id

        fact_orders = orders_enriched[[
            'order_id', 'order_ts', 'order_date', 'customer_id', 'store_id',
            'product_id', 'quantity', 'unit_price', 'discount_pct',
            'order_amount', 'payment_mode', 'order_status', 'channel',
            'delivery_minutes', 'net_revenue', 'gross_margin', 'within_15_min',
            'loaded_at', 'run_id'
        ]]

        print(f"[Transform] Fact orders ready: {len(fact_orders)} rows")
        return {
            'fact_orders': fact_orders.to_json(orient='split', date_format='iso'),
            'dim_customer': dim_customer.to_json(orient='split', date_format='iso'),
            'dim_product': dim_product.to_json(orient='split', date_format='iso'),
            'dim_store': dim_store.to_json(orient='split', date_format='iso'),
            'clean_payments': validated_data['clean_payments'],
            'total_extracted': validated_data['total_extracted'],
            'total_quarantined': validated_data['total_quarantined'],
        }

    @task
    def load_star_schema(transformed_data: dict):
        con = helpers.get_db_connection()

        fact_df = pd.read_json(io.StringIO(transformed_data['fact_orders']), orient='split')
        dim_c_df = pd.read_json(io.StringIO(transformed_data['dim_customer']), orient='split')
        dim_p_df = pd.read_json(io.StringIO(transformed_data['dim_product']), orient='split')
        dim_s_df = pd.read_json(io.StringIO(transformed_data['dim_store']), orient='split')

        dim_c_df['pincode'] = dim_c_df['pincode'].astype(str)
        dim_s_df['pincode'] = dim_s_df['pincode'].astype(str)

        con.execute('''
            CREATE TABLE IF NOT EXISTS etl_dim_customer (
                customer_id VARCHAR PRIMARY KEY,
                full_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                city VARCHAR,
                area VARCHAR,
                pincode VARCHAR,
                signup_date VARCHAR,
                loyalty_tier VARCHAR,
                is_active VARCHAR,
                loaded_at TIMESTAMP,
                run_id VARCHAR
            )
        ''')
        con.execute('DELETE FROM etl_dim_customer WHERE customer_id IN (SELECT customer_id FROM dim_c_df)')
        con.execute('INSERT INTO etl_dim_customer SELECT * FROM dim_c_df')

        con.execute('''
            CREATE TABLE IF NOT EXISTS etl_dim_product (
                product_id VARCHAR PRIMARY KEY,
                product_name VARCHAR,
                category VARCHAR,
                sub_category VARCHAR,
                brand VARCHAR,
                mrp DOUBLE,
                cost_price DOUBLE,
                unit VARCHAR,
                is_perishable BOOLEAN,
                launch_date VARCHAR,
                loaded_at TIMESTAMP,
                run_id VARCHAR
            )
        ''')
        con.execute('DELETE FROM etl_dim_product WHERE product_id IN (SELECT product_id FROM dim_p_df)')
        con.execute('INSERT INTO etl_dim_product SELECT * FROM dim_p_df')

        con.execute('''
            CREATE TABLE IF NOT EXISTS etl_dim_store (
                store_id VARCHAR PRIMARY KEY,
                store_name VARCHAR,
                zone VARCHAR,
                area VARCHAR,
                pincode VARCHAR,
                open_date VARCHAR,
                manager_name VARCHAR,
                capacity_orders_per_day BIGINT,
                loaded_at TIMESTAMP,
                run_id VARCHAR
            )
        ''')
        con.execute('DELETE FROM etl_dim_store WHERE store_id IN (SELECT store_id FROM dim_s_df)')
        con.execute('INSERT INTO etl_dim_store SELECT * FROM dim_s_df')

        con.execute('''
            CREATE TABLE IF NOT EXISTS etl_fact_orders (
                order_id VARCHAR PRIMARY KEY,
                order_ts TIMESTAMP,
                order_date DATE,
                customer_id VARCHAR,
                store_id VARCHAR,
                product_id VARCHAR,
                quantity BIGINT,
                unit_price DOUBLE,
                discount_pct DOUBLE,
                order_amount DOUBLE,
                payment_mode VARCHAR,
                order_status VARCHAR,
                channel VARCHAR,
                delivery_minutes DOUBLE,
                net_revenue DOUBLE,
                gross_margin DOUBLE,
                within_15_min BOOLEAN,
                loaded_at TIMESTAMP,
                run_id VARCHAR
            )
        ''')
        con.execute('DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM fact_df)')
        con.execute('INSERT INTO etl_fact_orders SELECT * FROM fact_df')

        fact_count = con.execute('SELECT COUNT(*) FROM etl_fact_orders').fetchone()[0]
        con.close()
        print(f"[Load] Star schema loaded idempotently. Total fact rows: {fact_count}")

        return {
            'fact_count': fact_count,
            'total_extracted': transformed_data['total_extracted'],
            'total_quarantined': transformed_data['total_quarantined'],
            'clean_payments': transformed_data['clean_payments'],
        }

    @task
    def reconcile_payments(load_summary: dict, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        con = helpers.get_db_connection()

        payments = pd.read_json(io.StringIO(load_summary['clean_payments']), orient='split')
        orders = con.execute('SELECT order_id, order_status, order_amount FROM etl_fact_orders').fetchdf()

        recon = orders.merge(payments, on='order_id', how='outer')

        mismatches = []
        for _, row in recon.iterrows():
            oid = row['order_id']
            ostatus = row.get('order_status')
            oamt = row.get('order_amount')
            pid = row.get('payment_id')
            pstatus = row.get('payment_status')
            pmode = row.get('payment_mode')
            pamt = row.get('amount_paid')

            reason = None
            if pd.isna(pid):
                if ostatus == 'DELIVERED':
                    reason = 'Delivered order has no payment record'
                elif ostatus == 'RETURNED':
                    reason = 'Returned order has no payment record'
            elif pd.isna(ostatus):
                reason = 'Payment exists for unrecorded or quarantined order'
            else:
                amt_diff = round(abs(float(oamt or 0) - float(pamt or 0)), 2)
                if ostatus == 'DELIVERED' and pstatus != 'SUCCESS':
                    reason = f"Delivered order with non-SUCCESS payment ({pstatus})"
                elif amt_diff > 0.01:
                    reason = f"Amount mismatch: order={oamt}, payment={pamt} (diff={amt_diff})"
                elif ostatus == 'CANCELLED' and pstatus == 'SUCCESS':
                    reason = 'Cancelled order has SUCCESS payment (unrefunded)'

            if reason:
                diff = round(float(oamt or 0) - float(pamt or 0), 2)
                mismatches.append({
                    'order_id': oid,
                    'order_status': ostatus if pd.notna(ostatus) else 'UNKNOWN',
                    'order_amount': oamt if pd.notna(oamt) else 0.0,
                    'payment_id': pid if pd.notna(pid) else 'NONE',
                    'payment_status': pstatus if pd.notna(pstatus) else 'NONE',
                    'payment_mode': helpers.standardize_payment_mode(pmode),
                    'amount_paid': pamt if pd.notna(pamt) else 0.0,
                    'amount_difference': diff,
                    'mismatch_reason': reason,
                    'reconciled_at': datetime.utcnow(),
                    'run_id': run_id
                })

        recon_df = pd.DataFrame(mismatches)

        con.execute('''
            CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (
                order_id VARCHAR,
                order_status VARCHAR,
                order_amount DOUBLE,
                payment_id VARCHAR,
                payment_status VARCHAR,
                payment_mode VARCHAR,
                amount_paid DOUBLE,
                amount_difference DOUBLE,
                mismatch_reason VARCHAR,
                reconciled_at TIMESTAMP,
                run_id VARCHAR
            )
        ''')
        con.execute('DELETE FROM etl_payment_reconciliation WHERE run_id = ?', [run_id])
        if len(recon_df) > 0:
            con.execute('INSERT INTO etl_payment_reconciliation SELECT * FROM recon_df')

        helpers.record_dq_result(
            con, run_id, 'nammamart_etl_ajay', 'order_payment_reconciliation',
            'Consistency', 'Critical', len(recon), len(recon_df),
            'WARN' if len(recon_df) > 0 else 'PASS'
        )

        con.close()
        print(f"[Reconcile] Found {len(recon_df)} reconciliation mismatches")
        return {
            'reconciliation_mismatches': len(recon_df),
            'fact_count': load_summary['fact_count'],
            'total_extracted': load_summary['total_extracted'],
            'total_quarantined': load_summary['total_quarantined'],
        }

    @task
    def generate_reports():
        con = helpers.get_db_connection()

        con.execute('''
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                f.order_date,
                s.zone,
                f.store_id,
                s.store_name,
                COUNT(f.order_id) AS delivered_orders,
                ROUND(SUM(f.net_revenue), 2) AS daily_net_revenue
            FROM etl_fact_orders f
            JOIN etl_dim_store s ON f.store_id = s.store_id
            WHERE f.order_status = 'DELIVERED'
            GROUP BY f.order_date, s.zone, f.store_id, s.store_name
            ORDER BY f.order_date, s.zone, f.store_id
        ''')

        con.execute('''
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                p.category,
                COUNT(f.order_id) AS total_orders,
                SUM(f.quantity) AS total_units_sold,
                ROUND(SUM(f.net_revenue), 2) AS category_revenue,
                ROUND(SUM(f.quantity * p.cost_price), 2) AS total_cost,
                ROUND(SUM(f.gross_margin), 2) AS gross_margin,
                ROUND((SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0)) * 100, 2) AS margin_pct
            FROM etl_fact_orders f
            JOIN etl_dim_product p ON f.product_id = p.product_id
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY category_revenue DESC
        ''')

        r1 = con.execute('SELECT * FROM etl_daily_zone_revenue LIMIT 5').fetchdf()
        r2 = con.execute('SELECT * FROM etl_category_margin').fetchdf()
        con.close()

        print("\n=== ETL REPORT: Daily Zone Revenue (Sample) ===")
        print(r1.to_string())
        print("\n=== ETL REPORT: Category Gross Margin ===")
        print(r2.to_string())
        return 'REPORTS_GENERATED'

    @task(trigger_rule='all_done')
    def audit_pipeline_run(recon_summary: dict, report_status: str, **context):
        run_id = context.get('run_id', f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        dag_id = 'nammamart_etl_ajay'

        start_time = context.get('data_interval_start', pendulum.now('Asia/Kolkata'))
        if hasattr(start_time, 'naive'):
            start_time = start_time.naive()
        end_time = datetime.utcnow()

        total_extracted = recon_summary.get('total_extracted', 0) if isinstance(recon_summary, dict) else 0
        total_loaded = recon_summary.get('fact_count', 0) if isinstance(recon_summary, dict) else 0
        total_quarantined = recon_summary.get('total_quarantined', 0) if isinstance(recon_summary, dict) else 0

        status = 'SUCCESS' if report_status == 'REPORTS_GENERATED' else 'FAILED'
        helpers.record_pipeline_run(
            run_id=run_id,
            dag_id=dag_id,
            start_time=start_time,
            end_time=end_time,
            rows_extracted=total_extracted,
            rows_loaded=total_loaded,
            rows_quarantined=total_quarantined,
            status=status
        )
        print(f"[Audit] Logged run {run_id} to pipeline_runs with status {status}")

    # DAG flow definition
    o = extract_orders()
    pay = extract_payments()
    c = extract_customers()
    prod = extract_products()
    s = extract_stores()

    v = validate_and_quarantine(o, pay, c, prod, s)
    t = transform(v)
    l = load_star_schema(t)
    r = reconcile_payments(l)
    rep = generate_reports()
    l >> rep
    audit_pipeline_run(r, rep)

nammamart_etl_dag = nammamart_etl_ajay()
