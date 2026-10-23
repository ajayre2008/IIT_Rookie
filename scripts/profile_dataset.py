"""
scripts/profile_dataset.py
--------------------------
Profiles all 6 CSV files for NammaMart assignment:
- Row counts, column types, null counts
- Duplicate keys
- Min / Max of numeric and date columns
- Distinct values of categorical columns
- Referential integrity checks
- Formula validations (order_amount formula, delivery SLA, payment amount match)
- Summary of data quality issues across all 6 dimensions
"""

import os
import re
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'include', 'data', 'nammamart')

def profile():
    print("=" * 80)
    print("NAMMAMART DATA PROFILING REPORT")
    print("=" * 80)

    files = {
        'orders.csv': os.path.join(DATA_DIR, 'orders.csv'),
        'payments.csv': os.path.join(DATA_DIR, 'payments.csv'),
        'customers.csv': os.path.join(DATA_DIR, 'customers.csv'),
        'products.csv': os.path.join(DATA_DIR, 'products.csv'),
        'stores.csv': os.path.join(DATA_DIR, 'stores.csv'),
        'orders_2026-10-01_increment.csv': os.path.join(DATA_DIR, 'orders_2026-10-01_increment.csv'),
    }

    dfs = {}
    for name, path in files.items():
        if os.path.exists(path):
            df = pd.read_csv(path)
            dfs[name] = df
            print(f"\n[FILE] {name}: {len(df)} rows, {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            nulls = df.isna().sum()
            nulls_with_counts = nulls[nulls > 0]
            if len(nulls_with_counts) > 0:
                print(f"  Null counts:\n{nulls_with_counts}")
            else:
                print("  No null values found.")
        else:
            print(f"MISSING FILE: {path}")

    # Check key uniqueness
    print("\n" + "=" * 80)
    print("PRIMARY KEY UNIQUENESS CHECKS")
    print("=" * 80)
    for name, key in [
        ('orders.csv', 'order_id'),
        ('payments.csv', 'payment_id'),
        ('customers.csv', 'customer_id'),
        ('products.csv', 'product_id'),
        ('stores.csv', 'store_id'),
        ('orders_2026-10-01_increment.csv', 'order_id'),
    ]:
        if name in dfs:
            df = dfs[name]
            dupes = df[df.duplicated(subset=[key], keep=False)]
            print(f"{name} on '{key}': {len(dupes)} duplicate rows ({len(df) - df[key].nunique()} non-unique keys)")
            if len(dupes) > 0:
                print(f"  Duplicate sample:\n{dupes[[key]].head()}")

    # Referential Integrity
    print("\n" + "=" * 80)
    print("REFERENTIAL INTEGRITY CHECKS")
    print("=" * 80)
    ord_df = dfs['orders.csv']
    cust_df = dfs['customers.csv']
    prod_df = dfs['products.csv']
    store_df = dfs['stores.csv']
    pay_df = dfs['payments.csv']

    cust_ids = set(cust_df['customer_id'].dropna().astype(str).str.strip())
    prod_ids = set(prod_df['product_id'].dropna().astype(str).str.strip())
    store_ids = set(store_df['store_id'].dropna().astype(str).str.strip())

    missing_custs = ord_df[~ord_df['customer_id'].astype(str).str.strip().isin(cust_ids)]
    print(f"Orders with invalid customer_id: {len(missing_custs)}")
    if len(missing_custs) > 0:
        print(f"  Sample: {missing_custs[['order_id', 'customer_id']].head(3)}")

    missing_prods = ord_df[~ord_df['product_id'].astype(str).str.strip().isin(prod_ids)]
    print(f"Orders with invalid product_id: {len(missing_prods)}")
    if len(missing_prods) > 0:
        print(f"  Sample: {missing_prods[['order_id', 'product_id']].head(3)}")

    missing_stores = ord_df[~ord_df['store_id'].astype(str).str.strip().isin(store_ids)]
    print(f"Orders with invalid store_id: {len(missing_stores)}")
    if len(missing_stores) > 0:
        print(f"  Sample: {missing_stores[['order_id', 'store_id']].head(3)}")

    ord_ids = set(ord_df['order_id'].dropna().astype(str).str.strip())
    missing_ord_pays = pay_df[~pay_df['order_id'].astype(str).str.strip().isin(ord_ids)]
    print(f"Payments with order_id not in orders.csv: {len(missing_ord_pays)}")
    if len(missing_ord_pays) > 0:
        print(f"  Sample: {missing_ord_pays[['payment_id', 'order_id', 'amount_paid']].head(3)}")

    # Business Rules Checks
    print("\n" + "=" * 80)
    print("BUSINESS RULES CHECKS (orders.csv)")
    print("=" * 80)
    # 1. order_amount formula: quantity * unit_price * (1 - discount_pct / 100)
    expected_amount = (ord_df['quantity'] * ord_df['unit_price'] * (1 - ord_df['discount_pct'] / 100.0)).round(2)
    diff = (ord_df['order_amount'] - expected_amount).abs()
    bad_amt = ord_df[diff > 0.05]
    print(f"Orders with formula mismatch (>0.05 diff): {len(bad_amt)}")
    if len(bad_amt) > 0:
        print(f"  Sample:\n{bad_amt[['order_id', 'quantity', 'unit_price', 'discount_pct', 'order_amount']].head(3)}")

    # 2. Valid order_status
    print(f"Distinct order_status: {ord_df['order_status'].value_counts(dropna=False).to_dict()}")

    # 3. Valid payment_mode in orders
    print(f"Distinct payment_mode in orders: {ord_df['payment_mode'].value_counts(dropna=False).to_dict()}")

    # 4. Delivery minutes check
    deliv_orders = ord_df[ord_df['order_status'].str.upper() == 'DELIVERED']
    bad_deliv_null = deliv_orders['delivery_minutes'].isna()
    bad_deliv_range = (deliv_orders['delivery_minutes'] < 0) | (deliv_orders['delivery_minutes'] > 120)
    print(f"Delivered orders with null delivery_minutes: {bad_deliv_null.sum()}")
    print(f"Delivered orders with delivery_minutes outside 0-120 min: {bad_deliv_range.sum()}")
    if bad_deliv_range.sum() > 0:
        print(f"  Sample out of range:\n{deliv_orders[bad_deliv_range][['order_id', 'delivery_minutes']].head(3)}")

    # 5. Invalid / future timestamps or invalid dates (like 31/09)
    bad_dates = ord_df[ord_df['order_ts'].astype(str).str.contains('31/09')]
    print(f"Orders with impossible date '31/09': {len(bad_dates)}")
    if len(bad_dates) > 0:
        print(f"  Sample: {bad_dates[['order_id', 'order_ts']].head(3)}")

    # 6. Negative or 0 quantity
    bad_qty = ord_df[ord_df['quantity'] <= 0]
    print(f"Orders with quantity <= 0: {len(bad_qty)}")

    # Customers checks
    print("\n" + "=" * 80)
    print("CUSTOMERS PROFILING")
    print("=" * 80)
    c_future = pd.to_datetime(cust_df['signup_date'], errors='coerce') > pd.to_datetime('2026-09-30')
    print(f"Customers with future signup_date: {c_future.sum()}")
    c_bad_phone = cust_df['phone'].astype(str).str.replace(r'\D', '', regex=True).str.len() != 10
    print(f"Customers with invalid phone length (!=10 digits): {c_bad_phone.sum()}")
    c_bad_email = cust_df['email'].isna() | (~cust_df['email'].astype(str).str.contains('@'))
    print(f"Customers with invalid email: {c_bad_email.sum()}")
    print(f"Distinct loyalty_tier: {cust_df['loyalty_tier'].value_counts(dropna=False).to_dict()}")

    # Products checks
    print("\n" + "=" * 80)
    print("PRODUCTS PROFILING")
    print("=" * 80)
    p_null_cat = prod_df['category'].isna()
    print(f"Products with null category: {p_null_cat.sum()}")
    p_bad_mrp = prod_df['mrp'] <= 0
    print(f"Products with non-positive MRP: {p_bad_mrp.sum()}")
    p_cost_gt_mrp = prod_df['cost_price'] > prod_df['mrp']
    print(f"Products with cost_price > mrp: {p_cost_gt_mrp.sum()}")

    # Stores checks
    print("\n" + "=" * 80)
    print("STORES PROFILING")
    print("=" * 80)
    s_null_zone = store_df['zone'].isna()
    print(f"Stores with null zone: {s_null_zone.sum()}")
    s_zero_cap = store_df['capacity_orders_per_day'] <= 0
    print(f"Stores with capacity <= 0: {s_zero_cap.sum()}")

    # Payments checks
    print("\n" + "=" * 80)
    print("PAYMENTS PROFILING")
    print("=" * 80)
    pay_bad_amt = pay_df['amount_paid'] <= 0
    print(f"Payments with amount <= 0: {pay_bad_amt.sum()}")
    print(f"Distinct payment_status: {pay_df['payment_status'].value_counts(dropna=False).to_dict()}")

    # Incremental feed checks
    print("\n" + "=" * 80)
    print("INCREMENTAL FEED CHECKS (orders_2026-10-01_increment.csv)")
    print("=" * 80)
    inc_df = dfs['orders_2026-10-01_increment.csv']
    existing_in_orders = inc_df[inc_df['order_id'].isin(ord_df['order_id'])]
    new_in_inc = inc_df[~inc_df['order_id'].isin(ord_df['order_id'])]
    print(f"Incremental rows matching existing order_id in orders.csv: {len(existing_in_orders)}")
    print(f"Incremental rows that are brand new: {len(new_in_inc)}")
    print(f"Statuses of matching existing orders in increment: {existing_in_orders['order_status'].value_counts().to_dict()}")

if __name__ == '__main__':
    profile()
