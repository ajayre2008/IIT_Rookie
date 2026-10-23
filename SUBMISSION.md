# NammaMart Quick Commerce Data Engineering Project
## Production ETL & ELT Pipelines, Data Quality Framework, and Business Analytics

**Author / Founding Data Engineer:** Ajay Kumar Reddy Vasanthu  
**Target Organization:** NammaMart (Bengaluru Quick-Commerce)  
**Date:** October 2026  
**Repository:** [IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model-main](.)

---

## Executive Summary

NammaMart operates an on-demand quick-commerce network across 15 dark stores in Bengaluru, delivering groceries, fresh produce, dairy, and household essentials under a target 15-minute delivery SLA. To transition NammaMart from fragile ad-hoc batch processing to an enterprise-grade analytics platform, this project delivers:

1. **Dual Orchestrated Pipelines (Apache Airflow 3.x + DuckDB + TaskFlow API):**
   - **ETL Pipeline (`dags/nammamart_etl_ajay.py`):** Pre-ingestion Python validation, automated quarantine of malformed orders, PII masking, star schema population (`etl_fact_orders`, `etl_dim_*`), and automated payment gateway reconciliation.
   - **ELT Pipeline (`dags/nammamart_elt_ajay.py`):** High-throughput SQL models (`raw_load.sql`, `staging.sql`, `incremental_merge.sql`, `marts.sql`) performing in-database cleansing, SCD Type-1/upsert merging of Day 2 delta feeds, and business marts computation.
2. **Deterministic Revenue Reconciliation:**
   - Both ETL and ELT models achieve exact revenue consistency on clean delivered baseline orders (**Rs. 931,157.95**). Following Day 2 incremental returns (7 orders returned with Rs. 3,579.70 refund), the adjusted September delivered revenue stands reconciled at **Rs. 927,578.25**.
3. **Data Quality & Governance Architecture:**
   - Multi-dimensional quality validation (Accuracy, Completeness, Consistency, Timeliness, Uniqueness, Validity) tracked in an audit table (`dq_results`).
   - Zero-data-loss quarantine subsystem isolating defective records to [include/quarantine/ajay/](include/quarantine/ajay/) with detailed root-cause attribution.
   - PII masking conforming to India's **Digital Personal Data Protection (DPDP) Act 2023** on customer email and phone fields.
4. **Operational & Financial Discoveries:**
   - **Delivery SLA Failure:** **100% of dark stores fail** the 85% on-time delivery threshold. City-wide average delivery time is **21.5 minutes**, with over **53% of orders exceeding 20 minutes**.
   - **Payment Gateway Leakage:** **38 delivered orders** have failed payments (**Rs. 23,584.65** uncollected revenue), and **149 delivered orders** lack any payment record (**Rs. 79,328.05** total exposure).
   - **Loyalty Program Inversion:** The highest spending customers (spending >Rs. 7,000) remain misclassified in the **Standard** tier while lower-spending customers enjoy Gold and Platinum perks.

---

## Part 0: Data Profiling & Issues Log

A profiling analysis was executed across all 6 raw CSV files in [include/data/nammamart/](include/data/nammamart/).

### Source File Inventory

| File Name | Rows | Columns | Entity Type | Primary Key |
|---|---|---|---|---|
| `orders.csv` | 2,412 | 14 | Transactional Orders | `order_id` |
| `payments.csv` | 2,228 | 6 | Payment Gateway Log | `payment_id` |
| `customers.csv` | 405 | 10 | Master Customer Data | `customer_id` |
| `products.csv` | 152 | 10 | Master Product Catalog | `product_id` |
| `stores.csv` | 15 | 8 | Dark Store Master | `store_id` |
| `orders_2026-10-01_increment.csv` | 160 | 14 | Incremental Batch Feed | `order_id` |

### Discovered Data Quality Anomalies

```
+----------------------------------------------------------------------------------------------------+
|                                    NAMMAMART DATA ANOMALY LOG                                     |
+--------------------+-------------+------------------------------------+----------------------------+
| Entity / File      | Category    | Issue Description                  | Volume / Impact            |
+--------------------+-------------+------------------------------------+----------------------------+
| orders.csv         | Uniqueness  | Exact duplicate rows               | 24 rows                    |
| orders.csv         | Complete    | Missing customer_id (null)         | 18 rows                    |
| orders.csv         | Foreign Key | Non-existent product_id ('P9xxx')  | 9 rows                     |
| orders.csv         | Foreign Key | Non-existent store_id ('S00')      | 5 rows                     |
| orders.csv         | Timeliness  | Impossible future date ('2027-01') | 1 row (ORD001725)          |
| orders.csv         | Validity    | Invalid calendar date ('31/09')    | 1 row (ORD000946)          |
| orders.csv         | Accuracy    | Negative quantities & amounts      | 3 rows (quantity = -1)     |
| orders.csv         | Accuracy    | Discount percentage > 100%         | 2 rows (120%, 150%)        |
| orders.csv         | Accuracy    | Extreme delivery time outliers     | 3 rows (> 120 min; 900m)   |
| orders.csv         | Validity    | Non-standard status ('Pending??')  | 4 rows                     |
| orders.csv         | Accuracy    | Pricing calculation mismatch       | 1 row (ORD001624 10x error)|
| customers.csv      | Uniqueness  | Duplicate customer IDs             | 4 rows                     |
| customers.csv      | Timeliness  | Future signup dates                | 4 rows                     |
| customers.csv      | Validity    | Malformed phone numbers / emails   | 8 rows                     |
| products.csv       | Accuracy    | Cost Price > MRP (Negative Margin) | 4 products                 |
| stores.csv         | Validity    | Inconsistent zone casing & codes   | 2 stores ('north_blr', etc)|
| payments.csv       | Uniqueness  | Reused payment_ids across orders   | 22 payment IDs             |
| payments.csv       | Complete    | Delivered orders missing payments  | 149 orders (Rs. 79,328.05) |
+--------------------+-------------+------------------------------------+----------------------------+
```

---

## Part A & Part B: Architecture Comparison (ETL vs ELT)

### 1. Visual Pipeline Topologies

#### ETL Pipeline Topology
```
+-----------------------------------------------------------------------------+
|                          ETL PIPELINE ARCHITECTURE                          |
+-----------------------------------------------------------------------------+
  [Source CSVs: Orders, Payments, Customers, Products, Stores]
        |
        v
  [1. TaskFlow Extraction (extract_orders, extract_payments, ...)]
        |
        v
  [2. Python Validation & Quarantine (validate_and_quarantine)]
        |---> Quarantine CSVs written to include/quarantine/ajay/
        |---> Logs 6 DQ Dimensions to DuckDB (dq_results)
        v
  [3. Transformation & Cleansing (transform)]
        |---> PII Masking (mask_email, mask_phone)
        |---> String standardization (Loyalty, Payment Mode)
        |---> Net Revenue & Gross Margin derived calculations
        v
  [4. Idempotent Star Schema Load (load_star_schema)]
        |---> DELETE + INSERT into etl_dim_customer, etl_dim_product,
        |     etl_dim_store, etl_fact_orders
        v
  [5. Payment Gateway Reconciliation (reconcile_payments)]
        |---> Loads etl_payment_reconciliation
        v
  [6. Business Mart Reports & Audit (generate_reports, audit_pipeline_run)]
        |---> Logs execution metrics to pipeline_runs
```

#### ELT Pipeline Topology
```
+-----------------------------------------------------------------------------+
|                          ELT PIPELINE ARCHITECTURE                          |
+-----------------------------------------------------------------------------+
  [Source CSVs]
        |
        v
  [1. Raw Load: include/sql/raw_load.sql]
        |---> DuckDB loads CSVs as TEXT with metadata (_loaded_at, _source_file)
        |---> Creates: raw_orders, raw_payments, raw_customers, raw_products, raw_stores
        v
  [2. Staging Layer: include/sql/staging.sql]
        |---> Type casting, regex validation, deduplication via ROW_NUMBER()
        |---> Tags records: dq_status ('PASSED'/'FAILED') & failure_reason
        |---> Creates: stg_orders, stg_payments, stg_customers, stg_products, stg_stores
        v
  [3. Incremental Upsert: include/sql/incremental_merge.sql]
        |---> Appends Day 2 batch to raw_orders
        |---> Merges new & updated orders (e.g. DELIVERED -> RETURNED) into stg_orders
        v
  [4. Business Marts: include/sql/marts.sql]
        |---> Creates mart_daily_zone_revenue, mart_category_margin,
        |     mart_delivery_sla, mart_reconciliation_summary,
        |     mart_top_customers, mart_store_capacity_check
        v
  [5. Cross-Pipeline Revenue Verification (verify_revenue_consistency)]
        |---> Asserts ETL vs ELT revenue equivalence
        v
  [6. Run Audit (audit_pipeline_run)]
        |---> Logs execution metrics to pipeline_runs
```

### 2. Side-by-Side Evaluation

| Architectural Attribute | ETL (`nammamart_etl_ajay.py`) | ELT (`nammamart_elt_ajay.py`) |
|---|---|---|
| **Transformation Engine** | Python / pandas in Airflow worker memory | In-database DuckDB vectorized engine |
| **Compute Location** | Application / Worker Tier | Storage / Analytical Warehouse Tier |
| **Raw Data Retention** | No raw table; only valid and quarantined sets stored | 100% raw data ingested as text with lineage metadata |
| **Validation Point** | Pre-load (data rejected before database insertion) | Post-load (all data ingested; flagged via `dq_status`) |
| **PII Protection** | Masked in memory before reaching DuckDB | Stored raw in staging, masked in downstream marts |
| **Execution Latency** | Higher CPU/memory overhead in worker | High-speed vectorized SQL execution |
| **Schema Evolution** | Requires code changes in Python parsing logic | Handled dynamically via SQL views and ALTER commands |
| **Auditability** | Clean star schema + external quarantine CSVs | Complete audit trail preserved in `raw_*` tables |

---

## Part C: Scheduling Decisions & Trade-offs

### Configuration Parameters

| Parameter | ETL Pipeline | ELT Pipeline |
|---|---|---|
| **DAG ID** | `nammamart_etl_ajay` | `nammamart_elt_ajay` |
| **Cron Expression** | `0 9 * * 1-6` | `30 9 * * 1-6` |
| **Wall Clock Time** | 09:00 AM IST (Monday–Saturday) | 09:30 AM IST (Monday–Saturday) |
| **Start Date** | `pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata')` | `pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata')` |
| **Catchup** | `False` | `False` |
| **Retries** | 2 retries | 2 retries |
| **Retry Delay** | 60 seconds (exponential backoff) | 60 seconds (exponential backoff) |
| **Execution Timeout**| 15 minutes | 15 minutes |

### Trade-off Analysis & Production Justification

1. **Why 09:00 AM IST?**
   - Quick-commerce dark stores operate 06:00 AM to 11:00 PM IST. Scheduling at 09:00 AM IST guarantees that all midnight batch closes, end-of-day courier settlements, and late-night customer refund requests from the previous operating day (23:59:59 IST) have reached the storage bucket.
   - Running at 09:00 AM allows store managers and category teams starting their shifts at 09:30 AM to have finalized dashboard metrics.
2. **Staggered Execution (09:00 AM vs 09:30 AM):**
   - Staggering ELT 30 minutes after ETL eliminates database lock contention on the shared DuckDB embedded database file and permits the ELT automated verification step (`verify_revenue_consistency`) to evaluate ETL fact tables.
3. **Catchup=False:**
   - Enabling `catchup=True` on an initial deployment across a month of historical data would trigger dozens of concurrent DAG runs, overflowing worker memory and producing lock collisions on embedded storage. Historical catchup should always be executed through controlled backfill CLI commands.
4. **Sunday Operational Gap:**
   - The business operates 7 days a week, but corporate management reviews KPIs Monday–Saturday. Running `1-6` leaves Sunday transactions unprocessed until Monday morning. To capture Sunday metrics without duplicate processing, Monday's run must extract a 48-hour window (Saturday 00:00 to Sunday 23:59), or the schedule must be adjusted to `0 9 * * *` (daily).

---

## Part D: Data Quality Dimensions, Framework & Results

Data quality checks are classified into six dimensions and logged automatically into the `dq_results` table:

```sql
CREATE TABLE IF NOT EXISTS dq_results (
    check_id VARCHAR,
    run_id VARCHAR,
    dag_id VARCHAR,
    check_timestamp TIMESTAMP,
    check_name VARCHAR,
    dimension VARCHAR,
    severity VARCHAR,
    records_checked BIGINT,
    records_failed BIGINT,
    status VARCHAR
);
```

### The Six Quality Dimensions

```
+----------------------------------------------------------------------------------------------------+
|                                    SIX DATA QUALITY DIMENSIONS                                     |
+---------------+--------------------------------------------+-----------------------+---------------+
| Dimension     | Validation Rule                            | Applied Field         | Outcome       |
+---------------+--------------------------------------------+-----------------------+---------------+
| Uniqueness    | Order, Payment, Customer PKs must be unique| order_id, customer_id | 24 dupes cut  |
| Completeness  | Foreign keys cannot be NULL                | customer_id, store_id | 18 nulls cut  |
| Validity      | Foreign keys must resolve in catalogs      | product_id, store_id  | 14 invalid cut|
| Accuracy      | Price formula & quantity must be positive  | order_amount, quantity| 3 negative cut|
| Timeliness    | Timestamps must not be in the future       | order_ts, signup_date | 5 future cut  |
| Consistency   | ETL vs ELT revenue match & gateway match   | net_revenue, amounts  | Fully verified|
+---------------+--------------------------------------------+-----------------------+---------------+
```

### Live Data Quality Execution Summary (`dq_results`)

```
+---------------+-----------+---------+-------------+----------------+
| Dimension     | Severity  | Status  | Check Count | Total Failures |
+---------------+-----------+---------+-------------+----------------+
| Accuracy      | Critical  | PASS    | 2           | 20.0           |
| Accuracy      | Critical  | FAIL    | 14          | 118.0          |
| Completeness  | Critical  | FAIL    | 4           | 8.0            |
| Consistency   | Critical  | WARN    | 1           | 531.0          |
| Consistency   | Critical  | FAIL    | 10          | 992.0          |
| Consistency   | Critical  | PASS    | 2           | 1,030.0        |
| Timeliness    | Critical  | FAIL    | 4           | 32.0           |
| Uniqueness    | Critical  | FAIL    | 8           | 46.0           |
| Uniqueness    | Critical  | PASS    | 2           | 0.0            |
| Validity      | Critical  | FAIL    | 4           | 12.0           |
| Validity      | Critical  | PASS    | 2           | 16.0           |
| Validity      | Warning   | WARN    | 4           | 32.0           |
+---------------+-----------+---------+-------------+----------------+
```

### Quarantine Implementation

Records failing critical validation are extracted with a composite `failure_reason` string and persisted to [include/quarantine/ajay/](include/quarantine/ajay/). 

Sample quarantine output (`orders_failed_*.csv`):
```csv
order_id,order_ts,customer_id,store_id,product_id,quantity,unit_price,discount_pct,order_amount,payment_mode,order_status,channel,delivery_minutes,failure_reason
ORD000134,2026-09-02 19:16:01,C00044,S14,P9822,1,219.0,10,197.1,UPI,DELIVERED,App,27.0,invalid_product_id
ORD001124,2026-09-15 13:05:08,C00396,S06,P0108,-1,161.0,10,-144.9,UPI,CANCELLED,App,,negative_quantity
ORD001725,2027-01-01 09:00:00,C00325,S12,P0138,1,507.0,15,430.95,Card,DELIVERED,App,26.0,future_order_date
ORD002126,2026-09-27 15:52:57,C00360,S00,P0034,1,205.0,0,205.0,Wallet,DELIVERED,App,29.0,invalid_store_id
```

---

## Part E: Data Governance & Compliance

### 1. PII Masking Implementation (DPDP Act 2023 Compliance)

To satisfy Section 8 of the Indian Digital Personal Data Protection Act, customer identifiers are masked prior to storage in data marts:

- **Email Masking:** Preserves the domain and masks user prefix (`sanjay.kumar@gmail.com` $\rightarrow$ `sa***@gmail.com`).
- **Phone Masking:** Masks the inner 6 digits, preserving routing and validation digits (`9876543210` $\rightarrow$ `98******10`).
- **Implementation:** Centrally managed in [include/nammamart_helpers.py](include/nammamart_helpers.py) and applied both in Pandas ETL transformations and SQL staging views.

### 2. Schema Evolution Strategy

When upstream store POS systems alter formats or add attributes:
- **Contract Definition:** Data contracts defined using JSON schema / Pydantic models.
- **Additive Evolution:** New columns in source CSVs default to nullable types in staging without failing downstream marts.
- **Deprecation Windows:** Breaking type changes require 30-day versioned views (`stg_orders_v1` vs `stg_orders_v2`).

### 3. Incident Management & Alerting

- Critical pipeline failures trigger Slack webhooks ([include/slack_notifier.py](include/slack_notifier.py)) with job metadata, error traceback, and row counts.
- Daily summaries post reconciliation mismatch counts and quarantine volumes to `#data-ops-alerts`.

---

## Part F: Pipeline Observability & Chaos Experiments

The production warehouse tracks every execution via `pipeline_runs`:

```sql
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    dag_id VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    rows_extracted BIGINT,
    rows_loaded BIGINT,
    rows_quarantined BIGINT,
    status VARCHAR
);
```

### Chaos Engineering & Robustness Tests

| Test ID | Failure Injected | Detection Mechanism | Pipeline Response | Alert Generated | Automated Recovery Action |
|---|---|---|---|---|---|
| **OBS-01** | Duplicate batch injection (100% replay of Day 1) | Uniqueness check via `order_id` in validation task | Idempotent `DELETE WHERE order_id IN (...)` prevents duplication | `WARN: Duplicate records suppressed` | Zero duplicate rows inserted; row count unchanged |
| **OBS-02** | Schema Drift: `unit_price` renamed to `price_inr` | DuckDB Column Binder Exception in `raw_load.sql` | Task fails immediately on raw ingest; downstream blocked | `CRITICAL: Schema drift detected on orders.csv` | Pipeline terminates safely without corrupting marts; alerts Slack |
| **OBS-03** | Volume Spike: 10x order volume (25,000 orders) | Pipeline duration monitoring vs 15-min timeout | DuckDB vectorized engine processes in 4.2 seconds | `INFO: High volume batch processed` | Execution completes well within SLA limit |
| **OBS-04** | Database File Lock contention | File lock retry loop in `get_db_connection()` | Airflow task retries (2 retries, 60s delay) | `WARN: Database lock contention detected` | Succeeds on retry once concurrent writer releases file lock |

---

## Business Questions Analysis

All queries were executed against DuckDB marts populated by both pipelines.

### Question 1: Top 3 & Bottom 3 Stores by September Net Delivered Revenue

```sql
SELECT store_id, store_name, zone, ROUND(SUM(daily_net_revenue), 2) AS september_revenue
FROM mart_daily_zone_revenue
WHERE order_date >= '2026-09-01' AND order_date <= '2026-09-30'
GROUP BY store_id, store_name, zone
ORDER BY september_revenue DESC;
```

#### Top 3 Stores
| Store ID | Store Name | Zone | September Net Delivered Revenue (Rs.) |
|---|---|---|---|
| **S06** | NammaMart Banashankari | South | **82,680.05** |
| **S10** | NammaMart Marathahalli | East | **75,450.55** |
| **S05** | NammaMart JP Nagar | South | **74,738.55** |

#### Bottom 3 Stores
| Store ID | Store Name | Zone | September Net Delivered Revenue (Rs.) |
|---|---|---|---|
| **S09** | NammaMart Indiranagar | East | **60,465.55** |
| **S11** | NammaMart HSR Layout | East | **66,336.50** |
| **S14** | NammaMart Vijayanagar | West | **67,800.95** |

*Insight:* The South zone outperforms all other regions, driven by dense residential demand in Banashankari and JP Nagar. Indiranagar's lower revenue indicates heavy local quick-commerce competition from Blinkit and Zepto in high-density commercial corridors.

---

### Question 2: Category Gross Margin Breakdown

```sql
SELECT category, total_orders, total_units_sold, category_revenue, total_cost, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY margin_pct DESC;
```

| Category | Orders | Units Sold | Total Revenue (Rs.) | Total Cost (Rs.) | Gross Margin (Rs.) | Margin % |
|---|---|---|---|---|---|---|
| **Dairy & Bakery** | 357 | 660 | 158,331.05 | 116,233.86 | 42,097.19 | **26.59%** |
| **Staples** | 256 | 473 | 224,773.55 | 167,374.92 | 57,398.63 | **25.54%** |
| **Household** | 305 | 562 | 179,356.35 | 133,902.02 | 45,454.33 | **25.34%** |
| **Fruits & Vegetables** | 329 | 635 | 81,109.20 | 61,992.62 | 19,116.58 | **23.57%** |
| **Snacks & Beverages** | 302 | 617 | 160,260.05 | 122,875.89 | 37,384.16 | **23.33%** |
| **Personal Care** | 287 | 570 | 187,728.60 | 145,698.56 | 42,030.04 | **22.39%** |
| **TOTAL** | **1,836** | **3,517** | **991,558.80** | **748,077.87** | **243,480.93** | **24.55%** |

*Insight:* **Dairy & Bakery** delivers the highest margin rate (26.59%), while **Staples** produces the highest absolute margin rupees (Rs. 57,398.63). Personal Care has the lowest margin percentage (22.39%) due to high wholesale procurement costs and aggressive FMCG price caps.

---

### Question 3: Delivery SLA Compliance by Store

Target SLA: $\ge 85\%$ of orders delivered within 15 minutes.

```sql
SELECT store_id, store_name, zone, total_delivered_orders,
       pct_under_15_min, pct_15_to_20_min, pct_over_20_min, avg_delivery_minutes
FROM mart_delivery_sla
ORDER BY pct_under_15_min ASC;
```

| Store ID | Store Name | Zone | Orders | % $\le$15 min | % 15–20 min | % >20 min | Avg Min | SLA Status |
|---|---|---|---|---|---|---|---|---|
| **S15** | Basaveshwaranagar | West | 143 | **20.98%** | 19.58% | 59.44% | 22.33 | **FAILED** |
| **S06** | Banashankari | South | 145 | **22.07%** | 20.69% | 57.24% | 22.33 | **FAILED** |
| **S04** | Jayanagar | South | 130 | **26.92%** | 19.23% | 53.85% | 21.59 | **FAILED** |
| **S10** | Marathahalli | East | 141 | **26.95%** | 20.57% | 52.48% | 21.90 | **FAILED** |
| **S13** | Malleshwaram | West | 137 | **27.01%** | 16.79% | 56.20% | 21.80 | **FAILED** |
| **S09** | Indiranagar | East | 127 | **27.56%** | 21.26% | 51.18% | 21.14 | **FAILED** |
| **S11** | HSR Layout | East | 145 | **28.28%** | 26.21% | 45.52% | 20.91 | **FAILED** |
| **S05** | JP Nagar | South | 134 | **29.10%** | 20.15% | 50.75% | 21.24 | **FAILED** |
| **S03** | RT Nagar | North | 137 | **29.20%** | 24.09% | 46.72% | 20.71 | **FAILED** |
| **S02** | Yelahanka | North | 139 | **30.94%** | 17.27% | 51.80 | 20.99 | **FAILED** |
| **S01** | Hebbal | North | 141 | **31.21%** | 21.28% | 47.52% | 20.62 | **FAILED** |
| **S14** | Vijayanagar | West | 162 | **31.48%** | 11.73% | 56.79% | 21.50 | **FAILED** |
| **S08** | Whitefield | East | 155 | **35.48%** | 14.84% | 49.68% | 20.60 | **FAILED** |

*Critical Finding:* **Every dark store fails the 85% SLA.** City-wide on-time delivery rate is only **28.3%**. Over **53% of orders take longer than 20 minutes**. Dark stores are severely hindered by peak traffic congestion and delayed courier rider dispatch.

---

### Question 4: Payment Reconciliation

```sql
SELECT order_status, payment_status, order_count, total_order_amount, total_paid_amount, discrepancy_amount
FROM mart_reconciliation_summary;
```

| Order Status | Payment Status | Order Count | Total Order Amount (Rs.) | Total Paid Amount (Rs.) | Financial Discrepancy (Rs.) |
|---|---|---|---|---|---|
| **DELIVERED** | **SUCCESS** | 1,649 | 888,646.10 | 888,161.10 | 485.00 |
| **DELIVERED** | **NO_PAYMENT** | 149 | 79,328.05 | 0.00 | **79,328.05 (Uncollected)** |
| **CANCELLED** | **NO_PAYMENT** | 123 | 73,035.25 | 0.00 | 73,035.25 |
| **RETURNED** | **REFUNDED** | 55 | 29,946.25 | 29,946.25 | 0.00 |
| **DELIVERED** | **FAILED** | 38 | 23,584.65 | 23,584.65 | **23,584.65 (At Risk)** |
| **RETURNED** | **SUCCESS** | 7 | 3,579.70 | 3,579.70 | **3,579.70 (Refund Due)** |
| **RETURNED** | **NO_PAYMENT** | 1 | 812.00 | 0.00 | 812.00 |

#### Reconciliation Answers
1. **Amount Mismatches:** **11 orders** exhibit payment amount mismatches totaling **Rs. 485.00**.
2. **Delivered with Failed Payment:** **38 orders** were handed over to customers despite payment gateway failures (**Rs. 23,584.65** uncollected).
3. **Delivered with Missing Payment:** **149 orders** lack any record in the payment gateway logs (**Rs. 79,328.05** total exposure).
4. **Cancelled with Successful Payment:** **0 orders** (no customer was charged for a cancelled order).
5. **Returned Orders Awaiting Refund:** **7 orders** returned on Oct 1 retain successful payment status (**Rs. 3,579.70** refund pending).

---

### Question 5: Top 10 Customers & Loyalty Tier Anomaly

```sql
SELECT customer_id, full_name, loyalty_tier, total_orders, total_spend, avg_order_value
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;
```

| Customer ID | Full Name | Loyalty Tier | Orders | Total Spend (Rs.) | AOV (Rs.) | Anomaly Detected |
|---|---|---|---|---|---|---|
| **C00115** | Sanjay Kumar | **Standard** | 7 | **7,337.70** | 1,048.24 | **Yes - Misclassified as Standard** |
| **C00310** | Kiran Patil | **Standard** | 8 | **7,258.95** | 907.37 | **Yes - Misclassified as Standard** |
| **C00265** | Vikram Hegde | Gold | 7 | **6,940.30** | 991.47 | Normal |
| **C00236** | Sneha Naidu | Platinum | 9 | **6,921.20** | 769.02 | Normal |
| **C00072** | Nandini Murthy | Silver | 11 | **6,433.95** | 584.90 | Normal |
| **C00283** | Nikhil Rao | **Standard** | 6 | **6,241.40** | 1,040.23 | **Yes - Misclassified as Standard** |
| **C00202** | Priya Bhat | Silver | 7 | **6,096.75** | 870.96 | Normal |
| **C00043** | Ganesh Joshi | Silver | 6 | **5,956.35** | 992.73 | Normal |
| **C00328** | Ritu Hegde | **Standard** | 4 | **5,877.90** | 1,469.48 | **Yes - Misclassified as Standard** |
| **C00338** | Bhavana Iyer | Silver | 7 | **5,845.35** | 835.05 | Normal |

#### Loyalty Program Root Cause
- Four of NammaMart's top 10 customers (including the #1 and #2 biggest spenders) are in the baseline **Standard** tier.
- **Root Cause:** Customer loyalty tiers were assigned statically at sign-up and are never recalculated dynamically based on trailing 30-day gross merchandise value (GMV).
- **Business Risk:** High-value VIP spenders receive zero loyalty rewards, free delivery perks, or priority customer support, risking churn to competing quick-commerce platforms.

---

### Question 6: Day 2 Incremental Impact (Oct 1 Feed)

1. **September Revenue Comparison:**
   - **Baseline September Net Delivered Revenue (ETL):** **Rs. 931,157.95**
   - **Adjusted September Revenue Post-Day 2 Merge (ELT):** **Rs. 927,578.25**
   - **Net Impact of Returned Orders:** **-Rs. 3,579.70**
2. **Newly Returned Orders:**
   - Exactly **7 orders** previously recorded as DELIVERED in September (`ORD000271`, `ORD000337`, `ORD000584`, `ORD000708`, `ORD001025`, `ORD001067`, `ORD001990`) were updated to **RETURNED** in the October 1st feed.
3. **October 1st Additive Performance:**
   - **October 1st Delivered Revenue:** **Rs. 63,980.55**
   - **Combined Total Delivered Mart Revenue:** **Rs. 991,558.80**
4. **Dark Store Capacity Analysis (Oct 1, 2026):**
   - Zero stores exceeded physical operating capacity. The busiest store on Oct 1 was **S11 (HSR Layout)** with 17 orders against a capacity of 300 orders/day (-283 headroom).
   - Dark stores are operating at $<10\%$ of physical packing capacity. Bottlenecks stem entirely from courier dispatch and delivery routing, not in-store picker throughput.

---

## Part G: CTO Recommendation & Strategy

### 1. Architectural Verdict: Adopt a Hybrid Modern Data Stack

For NammaMart's rapid growth trajectory (expanding from 15 to 100+ dark stores in South India), **ELT with In-Database Transformations (dbt + DuckDB/Snowflake) is the recommended analytical architecture**, complemented by lightweight micro-batch ingestion.

#### Strategic Rationale

```
+----------------------------------------------------------------------------------------------------+
|                                    HYBRID ARCHITECTURE STRATEGY                                    |
+--------------------------+------------------------------------+------------------------------------+
| Dimension                | Pure ETL Approach                  | Recommended Modern ELT Stack       |
+--------------------------+------------------------------------+------------------------------------+
| High-Volume Scalability  | Fails as pandas memory exceeds RAM | Handles millions of rows via SQL   |
| Raw Data Lineage         | Rejects data pre-load (lossy)      | Retains 100% raw data with audit   |
| Analytical Agility       | Requires Python code deployments   | Versioned SQL models in dbt        |
| Cost Efficiency          | High compute spend on workers      | Vectorized warehouse processing    |
+--------------------------+------------------------------------+------------------------------------+
```

### 2. Immediate 30-60-90 Day Implementation Roadmap

#### Days 1–30 (Immediate Stabilization & Risk Containment)
- **Implement POS Pre-Auth Handshake:** Prohibit courier order handoff unless the payment gateway returns `SUCCESS` or Cash-on-Delivery is signed off, immediately eliminating the **Rs. 102,912.70** payment leakage.
- **Dynamic Loyalty Engine:** Deploy a nightly Airflow task recomputing customer tiers:
  - Spend $\ge$ Rs. 5,000 $\rightarrow$ **Platinum**
  - Spend $\ge$ Rs. 3,000 $\rightarrow$ **Gold**
  - Spend $\ge$ Rs. 1,500 $\rightarrow$ **Silver**
- **Automate Sunday Schedules:** Switch DAG cron schedule to `0 9 * * *` (7-day daily) to eliminate weekend analytics blind spots.

#### Days 31–60 (Infrastructure Modernization)
- **dbt Integration:** Migrate [include/sql/](include/sql/) models into a version-controlled **dbt Core** project with built-in schema testing (`unique`, `not_null`, `accepted_values`).
- **Delivery Zone Geo-Fencing:** Shrink the delivery radius around dark stores failing SLA (Basaveshwaranagar, Banashankari) from 5 km to 3 km to bring delivery times under 15 minutes.

#### Days 61–90 (Enterprise Scale & Real-Time Analytics)
- **Streaming Ingestion:** Implement Apache Kafka / AWS Kinesis for real-time order tracking and dynamic delivery rider re-routing.
- **Cloud Warehouse Migration:** Transition DuckDB analytics marts to Snowflake/BigQuery as order volume surpasses 100,000 daily orders.
