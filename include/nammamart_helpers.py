"""
include/nammamart_helpers.py
-----------------------------
Helper functions for NammaMart Quick Commerce ETL & ELT Pipelines.
Supports PII masking, value standardization, path resolution,
data quality result persistence, and pipeline run auditing.
"""

import os
import re
from datetime import datetime
import pandas as pd
import duckdb

def get_base_dir() -> str:
    """Return base project directory, working both in Docker and locally."""
    if os.path.exists("/usr/local/airflow/include"):
        return "/usr/local/airflow"
    # fallback to local repo root (two levels up from this file)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def get_data_dir() -> str:
    return os.path.join(get_base_dir(), "include", "data", "nammamart")

def get_quarantine_dir() -> str:
    d = os.path.join(get_base_dir(), "include", "quarantine", "ajay")
    os.makedirs(d, exist_ok=True)
    return d

def get_warehouse_path() -> str:
    p = os.path.join(get_base_dir(), "include", "warehouse_ajay.duckdb")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p

def get_db_connection():
    return duckdb.connect(get_warehouse_path())

def mask_email(email) -> str:
    """Mask email as pr***@example.com. Preserves null/empty."""
    if pd.isna(email) or not email or str(email).strip().lower() in ("nan", "none", ""):
        return ""
    email_str = str(email).strip()
    if "@" not in email_str:
        return "***@masked.invalid"
    parts = email_str.split("@", 1)
    user, domain = parts[0], parts[1]
    if len(user) <= 2:
        masked_user = user[0] + "***" if len(user) > 0 else "***"
    else:
        masked_user = user[:2] + "***"
    return f"{masked_user}@{domain}"

def mask_phone(phone) -> str:
    """Mask phone as 98******21. Preserves null/empty."""
    if pd.isna(phone) or not phone or str(phone).strip().lower() in ("nan", "none", ""):
        return ""
    digits = re.sub(r"\D", "", str(phone).strip())
    if len(digits) < 4:
        return "******"
    return f"{digits[:2]}******{digits[-2:]}"

def standardize_payment_mode(mode) -> str:
    if pd.isna(mode):
        return "Unknown"
    m = str(mode).strip().lower()
    mapping = {
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cashh": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }
    return mapping.get(m, str(mode).strip().title())

def standardize_loyalty_tier(tier) -> str:
    if pd.isna(tier):
        return "Bronze"
    t = str(tier).strip().lower()
    mapping = {
        "bronze": "Bronze",
        "silver": "Silver",
        "gold": "Gold",
        "platinum": "Platinum",
    }
    return mapping.get(t, "Bronze")

def standardize_order_status(status) -> str:
    if pd.isna(status):
        return "UNKNOWN"
    s = str(status).strip().upper()
    if "DELIVERED" in s:
        return "DELIVERED"
    if "CANCELLED" in s:
        return "CANCELLED"
    if "RETURNED" in s:
        return "RETURNED"
    return s

def init_governance_tables(con=None):
    """Ensure dq_results and pipeline_runs tables exist."""
    close_con = False
    if con is None:
        con = get_db_connection()
        close_con = True
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR,
            dag_id VARCHAR,
            check_name VARCHAR,
            dimension VARCHAR,
            severity VARCHAR,
            rows_checked BIGINT,
            rows_failed BIGINT,
            status VARCHAR,
            checked_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            dag_id VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            rows_extracted BIGINT,
            rows_loaded BIGINT,
            rows_quarantined BIGINT,
            status VARCHAR
        )
    """)
    if close_con:
        con.close()

def record_dq_result(con, run_id: str, dag_id: str, check_name: str,
                     dimension: str, severity: str, rows_checked: int,
                     rows_failed: int, status: str):
    checked_at = datetime.utcnow()
    con.execute("""
        INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at])

def record_pipeline_run(run_id: str, dag_id: str, start_time: datetime,
                        end_time: datetime, rows_extracted: int,
                        rows_loaded: int, rows_quarantined: int,
                        status: str):
    init_governance_tables()
    con = get_db_connection()
    # Check if run_id already exists to make recording idempotent
    con.execute("""
        DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?
    """, [run_id, dag_id])
    con.execute("""
        INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status])
    con.close()
